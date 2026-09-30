#!/usr/bin/env python3
"""move n8n workflows between instances over the public API: dev -> git -> prod

    n8nctl.py pull     dev  workflows --tag stripe-xero
    n8nctl.py diff     prod workflows
    n8nctl.py push     prod workflows -m "what changed"
    n8nctl.py rollback prod workflows
    n8nctl.py creds    prod credentials.json --env ~/.config/stripe-xero/env

instance NAME comes from ~/.config/n8nctl/NAME.env: N8N_URL, N8N_API_KEY,
plus N8N_CA_FILE if the cert is from a private CA (Caddy's local one)

files keep credential names, not ids. push maps credentials and sub-workflow refs
to the target's ids, publishes callees before callers, won't overwrite edits made
on the target since the last push, and puts the old versions back if it fails

stdlib only
"""
import argparse
import copy
import difflib
import graphlib
import json
import os
import re
import ssl
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

# all the public API takes on write, the rest gets rejected
NODE_KEYS = {'id', 'name', 'type', 'typeVersion', 'position', 'parameters', 'credentials', 'webhookId', 'disabled',
             'notes', 'notesInFlow', 'executeOnce', 'alwaysOutputData', 'retryOnFail', 'maxTries',
             'waitBetweenTries', 'continueOnFail', 'onError'}
SETTINGS_KEYS = {'saveExecutionProgress', 'saveManualExecutions', 'saveDataErrorExecution', 'saveDataSuccessExecution',
                 'executionTimeout', 'errorWorkflow', 'timezone', 'executionOrder', 'binaryMode', 'callerPolicy',
                 'callerIds', 'availableInMCP', 'timeSavedMode', 'timeSavedPerExecution', 'redactionPolicy'}


class ApiError(Exception):
    pass


def read_env(path):
    env = {}
    for line in Path(path).expanduser().read_text().splitlines():
        key, sep, value = line.strip().partition('=')
        if sep and not key.startswith('#'):
            env[key.strip()] = value.strip().strip('\'"')
    return env


class Api:
    def __init__(self, instance):
        env = read_env(Path.home() / '.config' / 'n8nctl' / f'{instance}.env')
        self.instance, self.base, self.key = instance, env['N8N_URL'].rstrip('/') + '/api/v1', env['N8N_API_KEY']
        self.tls = ssl.create_default_context(cafile=env.get('N8N_CA_FILE'))

    def __call__(self, method, path, body=None, **query):
        url = self.base + path + ('?' + urllib.parse.urlencode(query) if query else '')
        req = urllib.request.Request(url, method=method, data=None if body is None else json.dumps(body).encode(),
                                     headers={'X-N8N-API-KEY': self.key, 'Content-Type': 'application/json',
                                              'Accept': 'application/json'})
        try:
            with urllib.request.urlopen(req, timeout=60, context=self.tls) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as e:
            raise ApiError(f'{method} {path}: HTTP {e.code} {e.read()[:400].decode(errors="replace")}') from None
        return json.loads(raw) if raw.strip() else None

    def all(self, path, **query):
        query['limit'] = 250
        while True:
            page = self('GET', path, **query)
            yield from page['data']
            if not page.get('nextCursor'):
                return
            query['cursor'] = page['nextCursor']

    def workflows(self):
        return [w for w in self.all('/workflows', excludePinnedData='true') if not w.get('isArchived')]


def canon(obj):
    return json.dumps(obj, indent=2, sort_keys=True, ensure_ascii=False) + '\n'


def slug(name):
    return re.sub(r'[^a-z0-9]+', '-', name.lower()).strip('-')


def portable(wf):
    """what goes to git: no credential ids, timestamps, versions or runtime state"""
    nodes = []
    for n in wf['nodes']:
        n = {k: v for k, v in n.items() if k in NODE_KEYS}
        if n.get('credentials'):
            n['credentials'] = {t: {'name': c['name']} for t, c in n['credentials'].items()}
        nodes.append(n)
    return {'id': wf['id'], 'name': wf['name'], 'active': bool(wf.get('active')),
            'tags': sorted(t['name'] for t in wf.get('tags') or []),
            'settings': {k: v for k, v in (wf.get('settings') or {}).items() if k in SETTINGS_KEYS},
            'nodes': nodes, 'connections': wf['connections']}


def called_id(node):
    """id of the workflow a node calls by a fixed ref (Execute Workflow, workflow tools), else None"""
    p = node.get('parameters') or {}
    ref = p.get('workflowId')
    value = ref.get('value') if isinstance(ref, dict) else ref
    if p.get('source', 'database') != 'database' or not isinstance(value, str) or not value or value.startswith('='):
        return None  # empty or an expression, nothing to map
    m = re.search(r'/workflow/([A-Za-z0-9]+)', value)  # "by URL" mode
    return m.group(1) if m else value


def to_target(wf, ids, creds, problems):
    """API body for the target, sub-workflow and credential refs rewritten to its ids"""
    def ref(i):
        if i not in ids:
            problems.append(f'{wf["name"]}: refers to workflow {i}, which is not in this directory')
        return ids.get(i, i)

    nodes = copy.deepcopy(wf['nodes'])
    for n in nodes:
        i = called_id(n)
        if i:
            rl = n['parameters']['workflowId']
            if isinstance(rl, dict):
                rl['value'] = ref(i)
                if rl.get('mode') == 'url':
                    rl['mode'] = 'id'
                if 'cachedResultUrl' in rl:
                    rl['cachedResultUrl'] = f'/workflow/{rl["value"]}'
            else:
                n['parameters']['workflowId'] = ref(i)
        for t, c in (n.get('credentials') or {}).items():
            cid = creds.get((t, c['name']))
            if cid is None:
                problems.append(f'{wf["name"]} / {n["name"]}: no {t} credential named "{c["name"]}" on the target')
            n['credentials'][t] = {'id': cid, 'name': c['name']}
    settings = dict(wf['settings'])
    if settings.get('errorWorkflow'):
        settings['errorWorkflow'] = ref(settings['errorWorkflow'])
    if settings.get('callerIds'):
        settings['callerIds'] = ','.join(ref(i.strip()) for i in settings['callerIds'].split(',') if i.strip())
    return {'name': wf['name'], 'nodes': nodes, 'connections': wf['connections'], 'settings': settings}


def current(wf):
    """target workflow in to_target() shape plus live state, to compare"""
    return {'name': wf['name'], 'nodes': [{k: v for k, v in n.items() if k in NODE_KEYS} for n in wf['nodes']],
            'connections': wf['connections'],
            'settings': {k: v for k, v in (wf.get('settings') or {}).items() if k in SETTINGS_KEYS},
            'active': bool(wf.get('active')), 'tags': sorted(t['name'] for t in wf.get('tags') or [])}


def publish_order(files):
    """names sorted callees first, n8n won't publish a caller before its callee"""
    by_id = {wf['id']: name for name, wf in files.items()}
    graph = {name: {by_id[i] for n in wf['nodes'] if (i := called_id(n)) in by_id and by_id[i] != name}
             for name, wf in files.items()}
    return list(graphlib.TopologicalSorter(graph).static_order())


def load_dir(d):
    files = {}
    for p in sorted(d.glob('*.json')):
        wf = json.loads(p.read_text())
        if wf['name'] in files:
            raise SystemExit(f'two files hold workflow {wf["name"]!r}')
        files[wf['name']] = wf
    if not files:
        raise SystemExit(f'no workflow files in {d}')
    return files


def state_file(d, instance):
    return d / '.n8nctl' / f'{instance}.json'


def load_state(d, instance):
    p = state_file(d, instance)
    return json.loads(p.read_text()) if p.exists() else {}


def save_state(d, instance, state, api):
    live = {w['name']: w for w in api.workflows()}
    state['workflows'] = {name: {'id': live[name]['id'], 'versionId': live[name]['versionId']}
                          for name in load_dir(d) if name in live}
    p = state_file(d, instance)
    p.parent.mkdir(exist_ok=True)
    p.write_text(canon(state))


def release_label(d):
    try:
        git = ['git', '-C', str(d)]
        sha = subprocess.run(git + ['rev-parse', '--short', 'HEAD'], capture_output=True, text=True, check=True).stdout
        dirty = subprocess.run(git + ['status', '--porcelain', '--', '.'], capture_output=True, text=True).stdout
        return f'n8nctl {sha.strip()}{"+dirty" if dirty.strip() else ""}'
    except (OSError, subprocess.CalledProcessError):
        return f'n8nctl {time.strftime("%Y-%m-%d %H:%M")}'


def pull(api, d, tag):
    d.mkdir(parents=True, exist_ok=True)
    query = {'excludePinnedData': 'true', **({'tags': tag} if tag else {})}
    written = set()
    for wf in api.all('/workflows', **query):
        if wf.get('isArchived'):
            continue
        path = d / f'{slug(wf["name"])}.json'
        if path.name in written:
            raise SystemExit(f'two workflows map to {path.name}, rename one')
        path.write_text(canon(portable(wf)))
        written.add(path.name)
    for p in d.glob('*.json'):  # drop files for workflows that are gone from the instance
        if p.name not in written and {'nodes', 'connections'} <= json.loads(p.read_text()).keys():
            p.unlink()
            print(f'removed {p.name}')
    save_state(d, api.instance, load_state(d, api.instance), api)  # files match these versions now
    print(f'pulled {len(written)} workflows from {api.instance} into {d}')


def plan(api, d):
    """compare the dir with the target, returns what push needs and what blocks it"""
    files = load_dir(d)
    by_name = {}
    for wf in api.workflows():
        by_name.setdefault(wf['name'], []).append(wf)
    creds = {(c['type'], c['name']): c['id'] for c in api.all('/credentials')}
    problems, target = [], {}
    for name in files:
        if len(by_name.get(name, [])) > 1:
            problems.append(f'{name}: {len(by_name[name])} workflows with this name on {api.instance}')
        elif name in by_name:
            target[name] = by_name[name][0]
    ids = {wf['id']: target[name]['id'] if name in target else f'<new {name}>' for name, wf in files.items()}
    known = load_state(d, api.instance).get('workflows', {})
    changes = {}
    for name, wf in files.items():
        want = canon({**to_target(wf, ids, creds, problems), 'active': wf['active'], 'tags': wf['tags']})
        cur = target.get(name)
        have = canon(current(cur)) if cur else ''
        if want != have:
            changes[name] = (have, want)
            seen = known.get(name, {}).get('versionId')
            if cur and seen and seen != cur['versionId']:
                problems.append(f'{name}: edited on {api.instance} after the last push; pull that edit or use --force')
    return files, target, creds, ids, changes, problems


def diff(api, d):
    *_, changes, problems = plan(api, d)
    for name, (have, want) in changes.items():
        print(f'{"update" if have else "create"}: {name}')
        sys.stdout.writelines(difflib.unified_diff(have.splitlines(True), want.splitlines(True),
                                                   f'{api.instance}/{name}', f'files/{name}', n=2))
    for p in problems:
        print(f'problem: {p}')
    print(f'{len(changes)} to change on {api.instance}' if changes else f'{api.instance} matches {d}')
    return 1 if changes or problems else 0


def push(api, d, message, force, dry_run):
    files, target, creds, ids, changes, problems = plan(api, d)
    if force:
        problems = [p for p in problems if 'after the last push' not in p]
    for name, (have, _) in changes.items():
        print(f'{"update" if have else "create"}: {name}')
    if problems:
        raise SystemExit('nothing pushed:\n  ' + '\n  '.join(problems))
    if not changes:
        if not dry_run:  # whatever was edited there equals the files, nothing to protect
            save_state(d, api.instance, load_state(d, api.instance), api)
        print(f'{api.instance} already matches {d}')
        return
    if dry_run:
        return

    label, order = release_label(d), [n for n in publish_order(files) if n in changes]
    prev, tag_ids = {}, None
    try:
        for name in order:  # create the missing ones first so refs have ids to point to
            if name in target:
                cur = target[name]
                prev[name] = {'id': cur['id'], 'created': False, 'versionId': cur['versionId'],
                              'activeVersionId': cur.get('activeVersionId'), 'settings': current(cur)['settings']}
            else:
                wf = api('POST', '/workflows', {'name': name, 'nodes': [], 'connections': {}, 'settings': {}})
                target[name], ids[files[name]['id']] = wf, wf['id']
                prev[name] = {'id': wf['id'], 'created': True}
        for name in order:
            wid, wf = target[name]['id'], files[name]
            # plain PUT republishes a live workflow right away, keep it a draft till callees are live
            api('PUT', f'/workflows/{wid}', to_target(wf, ids, creds, []), publishIfActive='false')
            if tag_ids is None:
                tag_ids = {t['name']: t['id'] for t in api.all('/tags')}
            for t in wf['tags']:
                if t not in tag_ids:
                    tag_ids[t] = api('POST', '/tags', {'name': t})['id']
            api('PUT', f'/workflows/{wid}/tags', [{'id': tag_ids[t]} for t in wf['tags']])
            if wf['active']:
                api('POST', f'/workflows/{wid}/publish', {'name': label, 'description': message})
            elif target[name].get('active'):
                api('POST', f'/workflows/{wid}/unpublish')
            print(f'  {"published" if wf["active"] else "saved"} {name}')
    except ApiError as e:
        print(f'push failed: {e}\nputting the previous versions back', file=sys.stderr)
        restore(api, prev, order)
        save_state(d, api.instance, load_state(d, api.instance), api)  # keep the last good push for rollback
        raise
    save_state(d, api.instance, {'last_push': {'at': time.strftime('%Y-%m-%dT%H:%M:%S%z'), 'label': label,
                                               'message': message, 'order': order, 'prev': prev}}, api)
    print(f'pushed {len(order)} workflows to {api.instance} as "{label}"')


def restore(api, prev, order):
    """put back drafts and live versions saved before a push, best effort, says what it couldn't do"""
    failed = []

    def attempt(what, *call, **query):
        try:
            api(*call, **query)
        except ApiError as e:
            failed.append(f'{what}: {e}')

    for name in order:  # callees first, same as publish
        p = prev.get(name)
        if not p or p['created']:
            continue
        try:
            v = api('GET', f'/workflows/{p["id"]}/versions/{p["versionId"]}')
        except ApiError as e:
            failed.append(f'{name}: {e}')
            continue
        attempt(name, 'PUT', f'/workflows/{p["id"]}',
                {'name': name, 'nodes': v['nodes'], 'connections': v['connections'], 'settings': p['settings']},
                publishIfActive='false')
        if p['activeVersionId']:
            attempt(name, 'POST', f'/workflows/{p["id"]}/publish',
                    {'versionId': p['activeVersionId'], 'name': 'n8nctl rollback'})
        else:
            try:
                api('POST', f'/workflows/{p["id"]}/unpublish')
            except ApiError:
                pass  # wasn't published
    for name in reversed(order):  # created by the push, callers are gone so archive them
        p = prev.get(name)
        if p and p['created']:
            try:
                api('POST', f'/workflows/{p["id"]}/unpublish')
            except ApiError:
                pass
            attempt(name, 'POST', f'/workflows/{p["id"]}/archive')
    for f in failed:
        print(f'  could not restore {f}', file=sys.stderr)
    print('restored' if not failed else 'restore incomplete, see above', file=sys.stderr)


def rollback(api, d):
    state = load_state(d, api.instance)
    last = state.pop('last_push', None)
    if not last:
        raise SystemExit(f'no push to roll back on {api.instance}')
    print(f'rolling back "{last["label"]}" ({last["at"]}): {", ".join(last["order"])}')
    restore(api, last['prev'], last['order'])
    save_state(d, api.instance, state, api)


def creds(api, spec_path, env_files):
    """create or update credentials from a spec with ${VARS}, secrets stay in env files"""
    env = dict(os.environ)
    for f in env_files:
        env.update(read_env(f))
    missing = set()

    def var(m):
        if not env.get(m[1]):
            missing.add(m[1])
        return env.get(m[1], '')

    def expand(v):
        return re.sub(r'\$\{(\w+)\}', var, v) if isinstance(v, str) else v

    spec = json.loads(Path(spec_path).read_text())
    data = {name: {k: expand(v) for k, v in c['data'].items()} for name, c in spec.items()}
    if missing:
        raise SystemExit('not set or empty: ' + ', '.join(sorted(missing)))
    have = {(c['type'], c['name']): c['id'] for c in api.all('/credentials')}
    for name, c in spec.items():
        cid = have.get((c['type'], name))
        if cid:
            api('PATCH', f'/credentials/{cid}', {'data': data[name]})
        else:
            api('POST', '/credentials', {'name': name, 'type': c['type'], 'data': data[name]})
        print(f'{"updated" if cid else "created"} {c["type"]} "{name}"')


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest='cmd', required=True)
    for cmd in ('pull', 'diff', 'push', 'rollback', 'creds'):
        p = sub.add_parser(cmd)
        p.add_argument('instance')
        p.add_argument('path', type=Path)
        if cmd == 'pull':
            p.add_argument('--tag', help='only workflows with this tag')
        if cmd == 'push':
            p.add_argument('-m', '--message', default='')
            p.add_argument('--force', action='store_true', help='overwrite edits made on the target since the last push')
            p.add_argument('--dry-run', action='store_true')
        if cmd == 'creds':
            p.add_argument('--env', action='append', default=[], help='env file with the secret values')
    a = ap.parse_args()
    api = Api(a.instance)
    try:
        if a.cmd == 'pull':
            pull(api, a.path, a.tag)
        elif a.cmd == 'diff':
            sys.exit(diff(api, a.path))
        elif a.cmd == 'push':
            push(api, a.path, a.message, a.force, a.dry_run)
        elif a.cmd == 'rollback':
            rollback(api, a.path)
        else:
            creds(api, a.path, a.env)
    except ApiError as e:
        raise SystemExit(str(e))


if __name__ == '__main__':
    main()
