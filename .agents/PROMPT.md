0. Confirm that you've loaded @AGENTS.md, have reviewed updates appended recently to `.agents/progress.txt`, and that we're in a new clean `jj` commit. If not, we will need to finish the previous commit and then make a new one before proceeding.
1. Find the first story in `.agents/prd.json` where `passes: false`, declare your intent for this commit with `jj describe` in the style discussed in @AGENTS.md, and then proceed to working only on that story. Stories are ordered by dependency, so always work on the first incomplete one. Implementation should follow all guidelines from @AGENTS.md.
2. Check that the code passes all checks, including formatting, clippy, tests, doc building, etc., with `just check`. No code may be committed that does not pass all checks.
3. Solicit feedback from @nick-isms, @testing-guru, and @semver-nag to ensure the code is high-quality. REMINDER: all code, before and after addressing code-review feedback, _must pass `just check`_.
4. Update the PRD: set `passes: true` for the completed story and add implementation notes to the `notes` field in `.agents/prd.json`.
5. Append your progress to `.agents/progress.txt`. Use this to leave a note for the next agent working on the codebase.
6. Make sure you haven't forgotten to allow any new paths (remember: no globs) in @.gitignore. Then, once everything is in your feature commit, make a new, clean commit with `jj new` for the next session.

ONLY WORK ON A SINGLE FEATURE.

If, while implementing a feature, you notice the whole PRD is complete, output <promise>COMPLETE</promise>.
