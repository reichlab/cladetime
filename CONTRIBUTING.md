# Contributing to Cladetime

Thank you for considering a contribution to Cladetime! We welcome all
collaborators, and this document lists some of the ways you can help us
improve the project.

## File issues

If you find a bug or have a feature request, feel free to open a
[GitHub issue](https://github.com/reichlab/cladetime/issues).

When reporting a bug, please provide a clear description of the problem so we
can triage effectively. If you have any related code or error messages, please
include them. When requesting a feature, provide as many details as possible
about your use case.

## Contribute code

If you'd like to contribute a bugfix or feature to Cladetime, please open an
issue first, so we can discuss the change and make sure it's not duplicating
work or adding a feature that doesn't align with the project's current goals.

(No issue needed for small changes like fixing typos or updating
documentation.)

### Project setup

Cladetime is written in Python. Feel free to use your preferred Python
installation/virtual environment/dependency management tools for project setup.
If you're new to Python, we recommend using `uv` to set up a development
environment on your local machine.

1. [Install `uv` on your machine](https://docs.astral.sh/uv/getting-started/installation/).
    - if you're installing `uv` from PyPi, we recommend
    [`pipx`](https://pipx.pypa.io/stable/installation/) or `pip`.
    - some Windows users have reported that they had to temporarily disable
    their anti-virus software before installing `uv`

2. Fork this repository and clone it onto your computer (Reich Lab team
members do not need to create a fork).

3. Navigate to the repository's root directory in your terminal.

4. Create a virtual environment. When creating the virtual environment, uv
will install the version of Python specified in
[.python-version](.python-version).

    ```bash
    uv venv --seed
    ```

5. Install the project's dependencies, including an editable copy of the
`cladetime` package.

    ```bash
    uv pip install -r requirements/requirements-dev.txt && uv pip install -e .
    ```

6. To ensure that your development environment is set up correctly, run the
test suite. If the tests pass, you're ready to start developing on Cladetime!

    ```bash
    uv run pytest
    ```

> [!TIP]
> Prefixing python commands with `uv run` instructs uv to run the command
> in the project's virtual environment, even if it isn't activated.

### Making and submitting changes

To get started, create a new branch for your changes. The branch name should
use the format
`<your initials>/<change-name>/<issue-number-if-applicable>`.

After you've created a new branch, make and test your changes.
We recommend making incremental changes and small commits so it's
easier for code reviewers to understand the updates.

Cladetime uses the following guidelines and tools to maintain code quality.
If you want to contribute but have questions about these tools, free to ask
in your issue or pull request (PR). We're happy to help!

- [Ruff](https://docs.astral.sh/ruff/), to lint and format code. GitHub will
automatically run Ruff on your pull request and fail if there are any issues.
You can run Ruff locally (and fix errors) with `uv run ruff check --fix`.
- [pytest](https://docs.pytest.org/en/stable/), to run tests. Large changes
and new features should be accompanied by new tests.
- Python module, class, and function docstrings follow the
[NumPy Style Guide](https://numpydoc.readthedocs.io/en/latest/format.html).
[Some good examples of NumPy-style docstrings](https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_numpy.html).
- [Read the Docs](https://readthedocs.org/), to build and host the project's
[documentation](https://cladetime.readthedocs.io/en/latest/).
If you're adding or updating features that impact the documentation, please
include those updates with your changes (see below for more information about
updating Cladetime's documentation).

When your changes are complete and you're ready to submit a [pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/about-pull-requests),
follow these steps:

1. If the repository has a CHANGELOG.md file, update it with a description of
your changes.
2. Push the changes to your forked repository.
3. From your forked repository on GitHub, create a pull request for your branch, targeting
the `main` branch of the original `cladetime` repository.

4. A team member will review the pull request and be in touch.

### Adding new dependencies

If your change requires a new dependency in the project, add it as follows:

1. Add the dependency to the `dependencies` section of [`pyproject.toml`](pyproject.toml).
2. Update the requirements files:

    ```bash
    uv pip compile pyproject.toml -o requirements/requirements.txt && uv pip compile pyproject.toml --extra dev -o requirements/requirements-dev.txt
    ```

3. Install the new dependency:

    ```bash
    uv pip install -r requirements/requirements-dev.txt
    ```

### Update documentation

Cladetime uses [Sphinx](https://www.sphinx-doc.org/en/master/) for
documentation. All related files are in the project's
[`docs`](docs/) directory. The documentation is hosted by Read the Docs, which creates
a new preview version of it whenever someone opens a pull request.

In other words, any documentation changes you've made will be automatically
available to preview as part of your PR.

We can't cover all the details of Sphinx here, but at a high-level:

- The documentation uses
[restructured text](https://www.sphinx-doc.org/en/master/usage/restructuredtext/basics.html)
(.rst), which is Sphinx's markup language.
- Cladetime uses the Sphinx
[autodoc](https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html)
extension to automatically generate documentation from Python docstrings.

As an optional step, you can build the documentation locally. This is a handy
way to troubleshoot Read the Docs build errors or to work iteratively instead
of waiting several minutes for the pull request to generate a preview.

1. Install the documentation dependencies:

    ```bash
    uv pip install -r requirements/requirements-docs.txt
    ```

2. Build the documentation:

    ```bash
    uv run sphinx-autobuild docs docs/_build/html
    ```

If you need to add or update any dependencies related to the documentation
(for example, adding a new Sphinx extension), the process is similar to
adding a new project dependency as described above:

1. Add the dependency to the `docs` section of [`pyproject.toml`](pyproject.toml).
2. Update the requirements files:

    ```bash
    uv pip compile pyproject.toml --extra docs -o requirements/requirements-docs.txt
    ```

3. Install the new dependency:

    ```bash
    uv pip install -r requirements/requirements-docs.txt
    ```
