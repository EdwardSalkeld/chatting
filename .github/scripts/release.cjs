// Publish only this repository's successful main-push CI results. No deployment
// repositories, external write tokens, or unchecked workflow artifacts are used.
module.exports = async function release({ github, context, core }) {
  const run = context.payload.workflow_run;
  const { owner, repo } = context.repo;
  if (!run || run.conclusion !== 'success' || run.event !== 'push' ||
      run.head_branch !== 'main' ||
      run.head_repository?.full_name !== `${owner}/${repo}`) {
    core.info('Not a successful main-push CI run from this repository; skipping.');
    return;
  }
  if (!/^[0-9a-f]{40}$/.test(run.head_sha) ||
      !Number.isSafeInteger(run.run_number) || run.run_number <= 0) {
    throw new Error('Invalid CI source SHA or run number');
  }
  const api = { owner, repo };
  const { data: branch } = await github.rest.repos.getBranch({ ...api, branch: 'main' });
  if (branch.commit.sha !== run.head_sha) {
    core.info('Main has advanced; leaving publication to its successful CI run.');
    return;
  }
  const tag = `v0.1.${run.run_number}`;
  const ref = `tags/${tag}`;
  let existing;
  try {
    existing = (await github.rest.git.getRef({ ...api, ref })).data;
  } catch (error) {
    if (error.status !== 404) throw error;
  }
  if (existing) {
    if (existing.object.type !== 'commit' || existing.object.sha !== run.head_sha) {
      throw new Error(`Refusing to overwrite ${tag}: it does not identify the tested commit`);
    }
  } else {
    await github.rest.git.createRef({ ...api, ref: `refs/${ref}`, sha: run.head_sha });
  }
  try {
    await github.rest.repos.getReleaseByTag({ ...api, tag });
    core.info(`${tag} already has a release; leaving it unchanged.`);
    return;
  } catch (error) {
    if (error.status !== 404) throw error;
  }
  await github.rest.repos.createRelease({
    ...api, tag_name: tag, target_commitish: run.head_sha,
    name: `Chatting ${tag}`, generate_release_notes: true, make_latest: 'legacy',
    body: `Source and Nix packages verified by CI run ${run.html_url}.`,
  });
  core.info(`Published ${tag} from ${run.head_sha}.`);
};
