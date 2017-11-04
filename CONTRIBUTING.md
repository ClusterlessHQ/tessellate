# How To Contribute

Welcome to the Heretical community.

This document will help answer common questions you may have preparing your contribution.

Keeping our projects exceptionally dependable and stable is paramount. But for them to continue to evolve and meet
expectations of its users, enhancements and fixes from third-parties are essential. Oftentimes meeting these goals is
not frictionless.

To mitigate this, we have set up guidelines and expectations for contributions, along with added clarity of our
commitment.

## Submitting Issues

Bye default we use Github to track all project issues, some sub-projects may use other means.

## Contribution Process

We have a multi-step process for contributions:

* Open an Issue with your feature or bug fix proposal
  ** Clearly describe the issue
  ** Make sure you note the earliest version that you know has the issue
* **If it is decided in the issue a fix or contribution is necessary, fork the repository on GitHub**
* Create a topic branch from where you want to base your work
  * This is usually the release branch (1.0) or release tag (1.0.1). Or wip releases (wip-1.0) 
  * Only target release branches if you are certain your fix must be on that branch
  * To quickly create a topic branch based on a release; `git checkout -b wip-1.0-topic origin/1.0`, 
    where 'topic' is describes your change
* Make commits of logical units, the fewer the better
  * A feature or fix per commit is reasonable, if not preferred
* Commit all changes making sure to sign-off those changes for the 
  [Developer Certificate of Origin](#developer-certification-of-origin-dco).
* Create a Pull Request for your change, following the instructions in the pull request template, if any.
* Perform a [Code Review](#code-review-process) with the project maintainers on the pull request.

### Code Review Process

Code review takes place in Github pull requests. See [this
article](https://help.github.com/articles/about-pull-requests/) if you're not familiar with Github Pull Requests.

The team will review any requests and provide feedback. Hopefully this will be minimal if we had a prior discussion
regarding the enhancements.

Regardless of the process, we reserve the right to decide when and if an enhancement will be included in any given
release. 

We prefer to push large changes into major or minor releases, and small changes in minor or maintenance releases. This
decision is based on the level and quality of changes that need to be acknowledged and absorbed, if at all, by the
end-users.

We may not merge a pull request directly from GitHub. Frequently commits are rebased and cleaned up on our side, and all
are committed with full attribution of the original author. 

### Developer Certification of Origin (DCO)

Licensing is very important to open source projects. It helps ensure the software continues to be available under the
terms that the author desired.

The license tells you what rights you have that are provided by the copyright holder. It is important that the
contributor fully understands what rights they are licensing and agrees to them. Sometimes the copyright holder isn't
the contributor, such as when the contributor is doing work on behalf of a company.

To make a good faith effort to ensure these criteria are met, we require the Developer Certificate of Origin (DCO)
process to be followed.

The DCO is an attestation attached to every contribution made by every developer. In the commit message of the
contribution, the developer simply adds a Signed-off-by statement and thereby agrees to the DCO, which you can find
below or at <http://developercertificate.org/>.

```
    Developer's Certificate of Origin 1.1
    
    By making a contribution to this project, I certify that:
    
    (a) The contribution was created in whole or in part by me and I
        have the right to submit it under the open source license
        indicated in the file; or
    
    (b) The contribution is based upon previous work that, to the
        best of my knowledge, is covered under an appropriate open
        source license and I have the right under that license to   
        submit that work with modifications, whether created in whole
        or in part by me, under the same open source license (unless
        I am permitted to submit under a different license), as
        Indicated in the file; or
    
    (c) The contribution was provided directly to me by some other
        person who certified (a), (b) or (c) and I have not modified
        it.
    
    (d) I understand and agree that this project and the contribution
        are public and that a record of the contribution (including
        all personal information I submit with it, including my
        sign-off) is maintained indefinitely and may be redistributed
        consistent with this project or the open source license(s)
        involved.
```

#### DCO Sign-Off Methods

The DCO requires a sign-off message in the following format appear on each commit in the pull request:

```
Signed-off-by: Jane Doe <jdoe@heretical.io>
```

You must use your real name (sorry, no pseudonyms or anonymous contributions.) 

The signed off text can either be manually added to your commit body, or you can add either **-s** or **--signoff** to
your usual git commit commands. If you forget to add the sign-off you can also amend a previous commit with the sign-off
by running **git commit --amend -s**. If you've pushed your changes to Github already you'll need to force push your
branch after this with **git push -f**.

### Obvious Fix Policy

Small contributions, such as fixing spelling errors, where the content is small enough to not be considered intellectual
property, can be submitted without signing the contribution for the DCO.

As a rule of thumb, changes are obvious fixes if they do not introduce any new functionality or creative thinking.
Assuming the change does not affect functionality, some common obvious fix examples include the following:

* Spelling / grammar fixes
* Typo correction, white space and formatting changes
* Comment clean up
* Bug fixes that change default return values or error codes stored in constants
* Adding logging messages or debugging output
* Changes to 'metadata' files like .gitignore, build scripts, etc.
* Moving source files from one directory or package to another

**Whenever you invoke the "obvious fix" rule, please say so in your commit message:**

```
    ------------------------------------------------------------------------
    commit 370adb3f82d55d912b0cf9c1d1e99b132a8ed3b5
    Author: Jane Doe <jdoe@heretical.io>
    Date:   Wed Sep 17 11:44:40 2017 -0700
    
      Fix typo in the README.
    
      Obvious fix.
    
    ------------------------------------------------------------------------
```
