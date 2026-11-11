const test = require('node:test');
const assert = require('node:assert/strict');
const release = require('./release.cjs');

const sha = 'a'.repeat(40);
const notFound = () => Object.assign(new Error('not found'), { status: 404 });
function fixture(changes = {}) {
  const calls = [];
  const context = {
    repo: { owner: 'example', repo: 'chatting' },
    payload: { workflow_run: {
      conclusion: 'success', event: 'push', head_branch: 'main',
      head_repository: { full_name: 'example/chatting' },
      head_sha: sha, run_number: 42, html_url: 'https://github.com/example/chatting/actions/runs/123',
      ...changes,
    } },
  };
  const record = name => async args => { calls.push([name, args]); return { data: {} }; };
  const github = { rest: {
    git: { getRef: async () => { throw notFound(); }, createRef: record('tag') },
    repos: {
      getBranch: async () => ({ data: { commit: { sha } } }),
      getReleaseByTag: async () => { throw notFound(); },
      createRelease: record('release'),
    },
  } };
  return { github, context, core: { info() {} }, calls };
}

test('publishes an immutable source tag and release for the tested SHA', async () => {
  const f = fixture();
  await release(f);
  assert.equal(f.calls.length, 2);
  assert.deepEqual(f.calls[0], ['tag', {
    owner: 'example', repo: 'chatting', ref: 'refs/tags/v0.1.42', sha,
  }]);
  assert.equal(f.calls[1][1].tag_name, 'v0.1.42');
  assert.equal(f.calls[1][1].target_commitish, sha);
  assert.equal(f.calls[1][1].generate_release_notes, true);
});

test('ignores failed, PR, fork, non-main and stale CI results', async () => {
  for (const change of [
    { conclusion: 'failure' }, { event: 'pull_request' }, { head_branch: 'feature' },
    { head_repository: { full_name: 'someone/chatting' } },
  ]) {
    const f = fixture(change);
    f.github.rest.repos.getBranch = async () => { assert.fail('must not call GitHub'); };
    await release(f);
    assert.deepEqual(f.calls, []);
  }
  const f = fixture();
  f.github.rest.repos.getBranch = async () => ({ data: { commit: { sha: 'b'.repeat(40) } } });
  await release(f);
  assert.deepEqual(f.calls, []);
});

test('rejects malformed source identifiers', async () => {
  for (const change of [{ head_sha: 'main' }, { run_number: 0 }, { run_number: '42' }]) {
    const f = fixture(change);
    await assert.rejects(release(f), /Invalid CI source/);
    assert.deepEqual(f.calls, []);
  }
});

test('reruns leave an existing matching release unchanged', async () => {
  const f = fixture();
  f.github.rest.git.getRef = async () => ({ data: { object: { type: 'commit', sha } } });
  f.github.rest.repos.getReleaseByTag = async () => ({ data: { tag_name: 'v0.1.42' } });
  await release(f);
  assert.deepEqual(f.calls, []);
});

test('recovers a tag created before a failed release request', async () => {
  const f = fixture();
  f.github.rest.git.getRef = async () => ({ data: { object: { type: 'commit', sha } } });
  await release(f);
  assert.deepEqual(f.calls.map(c => c[0]), ['release']);
});

test('never overwrites a tag at another commit', async () => {
  const f = fixture();
  f.github.rest.git.getRef = async () => ({ data: { object: { type: 'commit', sha: 'b'.repeat(40) } } });
  await assert.rejects(release(f), /Refusing to overwrite/);
  assert.deepEqual(f.calls, []);
});

test('API failures do not masquerade as a missing tag or release', async () => {
  for (const endpoint of ['getRef', 'getReleaseByTag']) {
    const f = fixture();
    f.github.rest.git.getRef = async () => ({ data: { object: { type: 'commit', sha } } });
    const service = endpoint === 'getRef' ? f.github.rest.git : f.github.rest.repos;
    service[endpoint] = async () => { throw Object.assign(new Error('unavailable'), { status: 503 }); };
    await assert.rejects(release(f), /unavailable/);
    assert.deepEqual(f.calls, []);
  }
});
