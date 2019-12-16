First off, thank you for considering contributing to *vertica-python* and helping make it even better than it is today!

This document will guide you through the contribution process. There are a number of ways you can help:

 - [Bug Reports](#bug-reports)
 - [Feature Requests](#feature-requests)
 - [Code Contributions](#code-contributions)
 
# Bug Reports

If you find a bug, submit an [issue](https://github.com/vertica/vertica-python/issues) with a complete and reproducible bug report. If the issue can't be reproduced, it will be closed. If you opened an issue, but figured out the answer later on your own, comment on the issue to let people know, then close the issue.

For issues (e.g. security related issues) that are **not suitable** to be reported publicly on the GitHub issue system, report your issues to [Vertica open source team](mailto:vertica-opensrc@microfocus.com) directly or file a case with Vertica support if you have a support account.

# Feature Requests

Feel free to share your ideas for how to improve *vertica-python*. We’re always open to suggestions.
You can open an [issue](https://github.com/vertica/vertica-python/issues)
with details describing what feature(s) you'd like added or changed.

If you would like to implement the feature yourself, open an issue to ask before working on it. Once approved, please refer to the [Code Contributions](#code-contributions) section.

# Code Contributions

## Step 1: Fork

Fork the project [on Github](https://github.com/vertica/vertica-python) and check out your copy locally.

```shell
git clone git@github.com:YOURUSERNAME/vertica-python.git
cd vertica-python
```

Your GitHub repository **YOURUSERNAME/vertica-python** will be called "origin" in
Git. You should also setup **vertica/vertica-python** as an "upstream" remote.

```shell
git remote add upstream git@github.com:vertica/vertica-python.git
git fetch upstream
```

### Configure Git for the first time

Make sure git knows your [name](https://help.github.com/articles/setting-your-username-in-git/ "Set commit username in Git") and [email address](https://help.github.com/articles/setting-your-commit-email-address-in-git/ "Set commit email address in Git"):

```shell
git config --global user.name "John Smith"
git config --global user.email "email@example.com"
```

## Step 2: Branch

Create a new branch for the work with a descriptive name:

```shell
git checkout -b my-fix-branch
```

## Step 3: Install dependencies

Install the Python dependencies for development:

```shell
pip install -r requirements-dev.txt
```

If you do Kerberos development, you need to install additional [dependencies](README.md#using-kerberos-authentication).

## Step 4: Get the test suite running

*vertica-python* comes with a test suite of its own, in the `vertica_python/tests` directory of the code base. It’s our policy to make sure all tests pass at all times.

We appreciate any and all [contributions to the test suite](#tests)! These tests use a Python module: [pytest](https://docs.pytest.org/en/latest/). You might want to check out the pytest documentation for more details.

There are two types of tests: unit tests and integration tests. Unit tests do simple unit testing of individual classes and functions, which do not require database connection. Integration tests need to connect to a Vertica database to run stuffs, so you must have access to a Vertica database. We recommend using a non-production database, because some tests need the superuser permission to manipulate global settings and potentially break that database. Heres one way to go about it:
- Download docker kitematic: https://kitematic.com/
- Spin up a vertica container (e.g. sumitchawla/vertica)

Spin up your Vertica database for integration tests and then config test settings:
* Here are default settings:
  ```sh
  host: 'localhost'
  port: 5433
  user: <current OS login user>
  database: <same as the value of user>
  password: ''
  log_dir: 'vp_test_log'  # all test logs would write to files under this directory
  log_level: logging.WARNING
  ```
* Override with a configuration file called `vertica_python/tests/common/vp_test.conf`. This is a file that would be ignored by git. We created an example `vertica_python/tests/common/vp_test.conf.example` for your reference.
  ```sh
  # edit under [vp_test_config] section
  VP_TEST_HOST=10.0.0.2
  VP_TEST_PORT=5000
  VP_TEST_USER=dbadmin
  VP_TEST_DATABASE=vdb1
  VP_TEST_PASSWORD=abcdef1234
  VP_TEST_LOG_DIR=my_log/year/month/date
  VP_TEST_LOG_LEVEL=DEBUG
  ```
* Override again with VP_TEST_* environment variables
  ```shell
  # Set environment variables in linux
  $ export VP_TEST_HOST=10.0.0.2
  $ export VP_TEST_PORT=5000
  $ export VP_TEST_USER=dbadmin
  $ export VP_TEST_DATABASE=vdb1
  $ export VP_TEST_PASSWORD=abcdef1234
  $ export VP_TEST_LOG_DIR=my_log/year/month/date
  $ export VP_TEST_LOG_LEVEL=DEBUG

  # Delete your environment variables after tests
  $ unset VP_TEST_PASSWORD
  ```

Tox (https://tox.readthedocs.io) is a tool for running those tests in different Python environments. *vertica-python*
includes a `tox.ini` file that lists all Python versions we test. Tox is installed with the `requirements-dev.txt`,
discussed above.

Edit `tox.ini` envlist property to list the version(s) of Python you have installed. Then you can run the **tox** command from any place in the *vertica-python* source tree. If VP_TEST_LOG_DIR sets to a relative path, it will be in the *vertica-python* directory no matter where you run the **tox** command.

Examples of running tests:

```bash
# Run all tests using tox:
tox

# Run tests on specified python versions with `tox -e ENV,ENV`
tox -e py27,py35

# Run specific tests by filename (e.g.) `test_notice.py`
tox -- vertica_python/tests/unit_tests/test_notice.py

# Run all unit tests on the python version 3.6:
tox -e py36 -- -m unit_tests

# Run all integration tests on the python version 3.4 with verbose result outputs:
tox -e py34 -- -v -m integration_tests

# Run an individual test on specified python versions.
# e.g.: Run the test `test_error_message` under `test_notice.py` on the python versions 2.7 and 3.5
tox -e py27,py35 -- vertica_python/tests/unit_tests/test_notice.py::NoticeTestCase::test_error_message
```

The arguments after the `--` will be substituted everywhere where you specify `{posargs}` in your test *commands* of
`tox.ini`, which are sent to pytest. See `pytest --help` to see all arguments you can specify after the `--`.

You might also run `pytest` directly, which will evaluate tests in your current Python environment, rather than across
the Python environments/versions that are enumerated in `tox.ini`.

For more usages about [tox](https://tox.readthedocs.io), see the Python documentation.

## Step 5: Implement your fix or feature

At this point, you're ready to make your changes! Feel free to ask for help; everyone is a beginner at first.

### License Headers

Every file in this project must use the following Apache 2.0 header (with the appropriate year or years in the "[yyyy]" box; if a copyright statement from another party is already present in the code, you may add the statement on top of the existing copyright statement):

```
Copyright (c) [yyyy] Micro Focus or one of its affiliates.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

### Commits

Make some changes on your branch, then stage and commit as often as necessary:

```shell
git add .
git commit -m 'Added two more tests for #166'
```

When writing the commit message, try to describe precisely what the commit does. The commit message should be in lines of 72 chars maximum. Include the issue number `#N`, if the commit is related to an issue.

### Tests

Add appropriate tests for the bug’s or feature's behavior, run the test suite again and ensure that all tests pass. Here is the guideline for writing test:
 - Tests should be easy for any contributor to run. Contributors may not get complete access to their Vertica database, for example, they may only have a non-admin user with write privileges to a single schema, and the database may not be the latest version. We encourage tests to use only what they need and nothing more.
 - If there are requirements to the database for running a test, the test should adapt to different situations and never report a failure. For example, if a test depends on a multi-node database, it should check the number of DB nodes first, and skip itself when it connects to a single-node database (see helper function `require_DB_nodes_at_least()` in `vertica_python/tests/integration_tests/base.py`).

## Step 6: Push and Rebase

You can publish your work on GitHub just by doing:

```shell
git push origin my-fix-branch
```

When you go to your GitHub page, you will notice commits made on your local branch is pushed to the remote repository.

When upstream (vertica/vertica-python) has changed, you should rebase your work. The **rebase** command creates a linear history by moving your local commits onto the tip of the upstream commits.

You can rebase your branch locally and force-push to your GitHub repository by doing:

```shell
git checkout my-fix-branch
git fetch upstream
git rebase upstream/master
git push -f origin my-fix-branch
```


## Step 7: Make a Pull Request

When you think your work is ready to be pulled into *vertica-python*, you should create a pull request(PR) at GitHub.

A good pull request means:
 - commits with one logical change in each
 - well-formed messages for each commit
 - documentation and tests, if needed

Go to https://github.com/YOURUSERNAME/vertica-python and [make a Pull Request](https://help.github.com/articles/creating-a-pull-request/) to `vertica:master`. 

### Sign the CLA
Before we can accept a pull request, we first ask people to sign a Contributor License Agreement (or CLA). We ask this so that we know that contributors have the right to donate the code. You should notice a comment from **CLAassistant** on your pull request page, follow this comment to sign the CLA electronically. 

### Review
Pull requests are usually reviewed within a few days. If there are comments to address, apply your changes in new commits, rebase your branch and force-push to the same branch, re-run the test suite to ensure tests are still passing. We care about quality, Vertica has internal test suites to run as well, so your pull request won't be merged until all internal tests pass. In order to produce a clean commit history, our maintainers would do squash merging once your PR is approved, which means combining all commits of your PR into a single commit in the master branch.

That's it! Thank you for your code contribution!

After your pull request is merged, you can safely delete your branch and pull the changes from the upstream repository.

