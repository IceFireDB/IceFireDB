# Contributing to [PROJECT'S NAME]

## Create an issue

- If you've encountered a bug, open a [Bug Report](https://github.com/PoCInnovation/$REPOSITORY/issues/new?assignees=&labels=&template=bug_report.md&title=).
- If you want [PROJECT'S NAME] to have a new fonctionality, open a [Feature Request](https://github.com/PoCInnovation/$REPOSITORY/issues/new?assignees=&labels=&template=feature_request.md&title=).

## Resolve an issue

Select an [issue](https://github.com/PoCInnovation/$REPOSITORY/issues) that you want to resolve.

The recommended workflow is to fork this repository and open pull requests from your fork.

### 1. Fork, clone & configure [PROJECT'S NAME] upstream

- Click on the _Fork_ button on GitHub
- Clone the original repository
- Add your repository as a new remote

```sh
# Clone original repository
git clone git@github.com:PoCInnovation/$REPOSITORY.git

# Add your fork as a remove
git remote add <fork_name> https://github.com/$YOUR_GITHUB_USER/$REPOSITORY.git
```

### 2. Create a pull request

```sh
# Create a new branch
git checkout -b my_branch

# Make changes to your branch
# ...

# Commit changes - remember to sign!
git commit -s

# Push your new branch
git push <fork name>

# Create a new pull request from https://github.com/PoCInnovation/$REPOSITORY/pulls
```

### 3. Update your pull request with latest changes

```sh
# Checkout main branch
git checkout main

# Pull origin's change
git pull

# Checkout your branch
git checkout my_branch

# Rebase your branch changes on top of the updated main branch
git rebase main

# Update your pull request with latest changes
git push -f <fork name>
```

## Commits

### DCO

Contributions to this project must be accompanied by a Developer Certificate of
Origin (DCO).

All commit messages must contain the Signed-off-by line with an email address that matches the commit author. When commiting, use the `--signoff` flag:

```sh
git commit -s
```

The Signed-off-by line must match the **author's real name**, otherwise the PR will be rejected.

### Commit messages

Please read first this article : [How to Write a Git Commit Message](https://chris.beams.io/posts/git-commit/).

Then, follow these guidelines:

- **Group Commits:** Each commit should represent a meaningful change. Instead, these commits should be squashed together into a single "Add Feature" commit.
> For instance, a PR should not look like :
> - 1) Add Feature X
> - 2) Fix Typo
> - 3) Changes to features X
> - 5) Bugfix for feature X
> - 6) Fix Linter 7)
> - ...

- Each commit should **work on its own**: it must compile, pass the linter and so on.
> This makes life much easier when using `git log`, `git blame`, `git bisect`, etc...\
> For instance, when doing a `git blame` on a file to figure out why a change was introduced, it's pretty meaningless to see a _Fix linter_ commit message. "Add Feature X" is much more meaningful.

- Use `git rebase -i main` to group commits together and rewrite their commit message

- To add changes to the previous commit, use `git commit --amend -s`. This will change the last commit (amend) instead of creating a new commit.

- Format: Use the imperative mood in the subject line: "If applied, this commit
  will _your subject line here_"

- Add the following prefixes to your commit message to help trigger [automated processes](https://www.conventionalcommits.org):
    - `docs:` for documentation changes only (e.g., `docs: Fix typo in X`);
    - `test:` for changes to tests only (e.g., `test: Check if X does Y`);
    - `chore:` general things that should be excluded (e.g., `chore: Clean up X`);
    - `ci:` for internal CI specific changes (e.g., `ci: Enable X for tests`);

> Made with :heart: by PoC