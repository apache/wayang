---
license: |
    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    (the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
layout: default
title: "Testing Releases"
previous:
    url: /how_contribute/reviewing_changes/
    title: Reviewing Changes
next:
    url: /how_contribute/user_libraries_wayang/
    title: User Libraries to Wayang
menus:
    contribute:
        weight: 7
---

# Release process

Wayang’s release process is community-oriented, and members of the community can vote on new releases on the [email list@dev](mailto:dev@wayang.apache.org). Wayang users are invited to subscribe to the email list \#Dev to receive announcements, and test their workloads on newer release and provide feedback on any performance or correctness issues found in the newer release.

## Preparing your system

As part of the release process, Maven will upload maven release artifacts to a so-called staging repository.

This can be thought of as an ad-hoc Maven repository that contains only the artifacts for one release. This helps reviewers to see what’s in the convenience maven package and to release that to the public repos with one click.

In order to be allowed to upload artifacts, your account has to be enabled for this, and you have to tell Maven about your credentials.

In order to do this, you should provide these credentials via `.m2/settings.xml`.

So if you don’t already have one, you should create a .m2 directory in your user home and inside that create a `settings.xml` file with at least this content:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<settings xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.1.1-SNAPSHOT http://maven.apache.org/xsd/settings-1.1.1-SNAPSHOT.xsd" xmlns="http://maven.apache.org/SETTINGS/1.1.1-SNAPSHOT"
xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    <servers>
    <!-- Apache Repo Settings -->
        <server>
            <id>apache.snapshots.https</id>
            <username>{user-id}</username>
            <password>{user-pass}</password>
        </server>
        <server>
            <id>apache.releases.https</id>
            <username>{user-id}</username>
            <password>{user-pass}</password>
        </server>
    </servers>
</settings>
```


This tells maven to use above credentials as soon as a repository with the id `apache.snapshots.https` or `apache.releases.https` is being used. For a release all you need is the releases repo, but it is good to have the other in place as it enables you to also deploy SNAPSHOTs from your system. There repos are defined in the apache parent pom and is identical for all Apache projects.

Additionally, all artifacts are automatically signed by the release build. In order to be able to do this you need to set up GPG.

The key being used to sign the artifacts will have to be linked to your Apache E-Mail ({apache-id}@apache.org) and verified by at least one fellow Apache committer (Ideally more) that have trusted keys themselves. Usually for this you have to get in touch - in real life - with any Apache committer with a trusted key. Attending an ApacheCon is usually a great way to do this as usually every ApacheCon has a Key Signing event in its schedule. He can then sign your key and hereby enable you to sign Apache release artifacts.

If you happen to have multiple keys, adding the following profile to your settings.xml should help:

```xml
<profile>
  <id>apache-release</id>
  <properties>
    <gpg.keyname>5C60D6B9</gpg.keyname><!-- Your GPG Keyname here -->
    <!-- Use an agent: Prevents being asked for the password during the build -->
    <gpg.useagent>true</gpg.useagent>
    <gpg.passphrase>topsecret-password</gpg.passphrase>
  </properties>
</profile>
```



## Creating a release branch

For each new release we create a new branch at the beginning of a code-freeze phase. So if currently the project version in develop is `0.6.0-SNAPSHOT`, we create a branch `rel/0.6`. When creating the branch is exactly the moment in which the version in develop is incremented to the next minor version.

This can and should be automated by the maven-release-plugin. Per default the plugin will ask for the working copy version during the build execution. This is the version the develop branch will be changed to.

In contrast to normal builds, it is important to enable all profiles when creating the branch as only this way will all modules versions be updated. Otherwise, the non-default modules on develop will reference the old version which will cause problems when building.

Being 0.6 the minor version
```shell
mvn release:branch -P scala-12 -DbranchName=rel/0.6
```
The plugin will then aks for the version:

```shell
What is the new working copy version for "Wayang"? (org.apache.wayang:wayang-parent) 0.6.0-SNAPSHOT: : 0.7.0-SNAPSHOT
```

## Preparing a release

Same as with creating the branch it is important to enable all profiles when creating the branch as only this way will all modules versions be updated. Otherwise, the non-default modules on develop will reference the old version which will cause problems when building. For people building with some additional profiles from the source release will be impossible.

As especially when switching a lot between different branches, it is recommended to do a clean checkout of the repository. Otherwise, a lot of directories can be left over, which would be included in the `source-release` zip. In order to prepare a `release-candidate`, the first step is switching to the corresponding `release-branch`.

After that, the following command will to all preparation steps for the release:

```shell
mvn release:prepare -P scala-12
```

(The -P scala-12 tells maven to activate all the profiles that partition the build and makes sure the versions of all modules are updated as part of the release) In general the plugin will now ask you 3 questions:

1. The version we want to release as (It will suggest the version you get by omitting the `-SNAPSHOT` suffix)

2. The name of the tag the release commit will be tagged with in the SCM (Name it `v{release-version}` (`v0.6.0` in our case)

3. The next development version (The version present in the pom after the release) (`{current-next-bugfix-version}` in our case)

Usually for 1 and 3 the defaults are just fine, make sure the tag name is correct as this usually is different.

What the plugin now does, is automatically execute the following operations:

1. Check we aren’t referencing any `SNAPSHOT` dependencies.

2. Update all pom versions to the release version.

3. Run a build with all tests

4. Commit the changes (commit message: [maven-release-plugin] prepare release `v0.6.0`)

5. Push the commit

6. Tag the commit

7. Update all poms to the next development version.

8. Commit the changes (commit message: [maven-release-plugin] prepare for next development iteration)

9. Push the commit

However, this just prepared the git repository for the release, we have to perform the release to produce and stage the release artifacts.

Please verify the git repository at: `https://gitbox.apache.org/repos/asf?p=incubator-wayang.git` is in the correct state. Please select the release branch and verify the commit log. It is important that the commit with the message "[maven-release-plugin] prepare release `v{release-version}`" is tagged with the release tag (in this case `v0.6.0`)

# What if something goes wrong?

If something goes wrong, you can always execute:

```shell
mvn release:rollback
```

It will change the versions back and commit and push things.

However, it will not delete the tag in GIT (locally and remotely). So you have to do that manually or use a different tag next time.

# Performing a release

This is done by executing another goal of the maven-release-plugin:

```shell
mvn release:perform
```

This executes automatically as all information it requires is located in the release.properties file the prepare-goal prepared.

The first step is that the perform-goal checks out the previously tagged revision into the root modules target/checkout directory. Here it automatically executes a maven build (You don’t have to do this, it’s just that you know what’s happening):

```shell
mvn clean deploy -P apache-release
```

As the apache-release profile is also activated, this builds and tests the project as well as creates the JavaDocs, Source packages and signs each of these with your PGP key.

We are intentionally not adding the other profiles, as these either produce binary artifacts that usually only work on the system they were compiled on (C++, .Net).

As this time the build is building with release versions, Maven will automatically choose the release url for deploying artifacts.

The way things are set up in the apache parent pom, is that release artifacts are deployed to a so-called staging repository.

You can think of a staging repository as a dedicated repository created on the fly as soon as the first artifact comes in.

After the build you will have a nice and clean Maven repository at `https://repository.apache.org/` that contains only artifacts from the current build.

At this point, it is important to log in to Nexus at `https://repository.apache.org/`, select Staging Repositories and find the repository with the name: `orgapachewayang-{somenumber}`.

Select that and click on the Close button.

Now Nexus will do some checks on the artifacts and check the signatures.

As soon as it’s finished, we are done on the Maven side and ready to continue with the rest of the release process.

A release build also produces a so-called source-assembly zip.

This contains all sources of the project and will be what’s actually the release from an Apache point of view and will be the thing we will be voting on.

This file will also be signed and SHA512 hashes will be created.

## Staging a release

Each new release and release-candidate has to be staged in the Apache SVN under:

* `https://dist.apache.org/repos/dist/dev/wayang/`

The directory structure of this directory is as follows:

```shell
./KEYS
./0.6.0/
./0.6.0/rc1
./0.6.0/rc1/README
./0.6.0/rc1/RELEASE_NOTES
./0.6.0/rc1/apache-wayang-0.6.0-source-release.zip
./0.6.0/rc1/apache-wayang-0.6.0-source-release.zip.asc
./0.6.0/rc1/apache-wayang-0.6.0-source-release.zip.sha512
```

You can generally import the stuff, by preparing a directory structure like above locally and then using svn import to do the importing:

```shell
cd ./{current-full-version}
svn import rc1 https://dist.apache.org/repos/dist/dev/wayang/{current-full-version}/rc1 -m"Staging of rc1 of Wayang {current-full-version}"
```

* The KEYS file contains the PGP public key which belongs to the private key used to sign the release artifacts.

If this is your first release be sure to add your key to this file. For the format have a look at the file itself. It should contain all the information needed.

Be sure to stage exactly the README and RELEASE_NOTES files contained in the root of your project. Ideally you just copy them there from there.

All three `-source-relese.zip` artifacts should be located in the directory: `target/checkout/target`

After committing these files to SVN you are ready to start the vote.

## Starting a vote on the mailing list

After staging the release candidate in the Apache SVN, it is time to actually call out the vote.

For this we usually send two emails. The following would be the one used to do our first TLP release:

E-Mail Topic:
<div style="border: 1px solid gray; padding: 1em;">
<pre>
[VOTE] Apache Wayang 1.X.X RC1
</pre>
</div>

Message:
<div style="border: 1px solid gray; padding: 1em;">
<pre>
Apache Wayang 1.X.X has been staged under [2] and it’s time to vote on accepting it for release. All Maven artifacts are available under [1].
Voting will be open for 72hr. A minimum of 3 binding +1 votes and more binding +1 than binding -1
are required to pass.

* Release tag: v0.6.0

Hash for the release tag: {replacethiswiththerealgitcommittag}

Per [3] "Before voting +1 PMC members are required to download
the signed source code package, compile it as provided, and test
the resulting executable on their own platform, along with also
verifying that the package meets the requirements of the ASF policy
on releases."

You can achieve the above by following [4].

[ ]  +1 accept (indicate what you validated - e.g. performed the non-RM items in [4])

[ ]  -1 reject (explanation required)


<a style="color: dodgerblue">1. https://repository.apache.org/content/repositories/orgapachewayang-{somefourdigitnumber}</a>
<a style="color: dodgerblue">2. https://dist.apache.org/repos/dist/dev/wayang/0.6.0/rc1</a>
<a style="color: dodgerblue">3. https://www.apache.org/dev/release.html#approving-a-release</a>

</pre>
</div>

As it is sometimes to do the vote counting, if voting and discussions are going on in the same thread, we send a second email:

E-Mail Topic:
<div style="border: 1px solid gray; padding: 1em;">
<pre>[DISCUSS] Apache Wayang 1.X.X RC1</pre>
</div>


Message:
<div style="border: 1px solid gray; padding: 1em;">
<pre>
This is the discussion thread for the corresponding VOTE thread.

Please keep discussions in this thread to simplify the counting of votes.

If you have to vote -1 please mention a brief description on why and then take the details to this thread.
</pre>
</div>



Now we have to wait 72 hours till we can announce the result of the vote. The vote passes, if at least 3 +1 votes are received and more +1 are received than -1.

## Releasing after a successful vote

As soon as the votes are finished, and the results were in favor of a release, the staged artifacts can be released. This is done by moving them inside the Apache SVN.

```shell
svn move -m "Release Apache Wayang 1.X.X" \
  https://dist.apache.org/repos/dist/dev/wayang/1.X.X/rc1 \
  https://dist.apache.org/repos/dist/release/wayang/1.X.X
```

This will make the release artifacts available and will trigger them being copied to mirror sites.

This is also the reason why you should wait at least 24 hours before sending out the release notification emails.

## Cleaning up older release versions

As a lot of mirrors are serving our releases, it is the Apache policy to clean old releases from the repo if newer versions are released.

This can be done like this:

```shell
svn delete https://dist.apache.org/repos/dist/release/wayang/{current-full-version}/ -m"deleted version {current-full-version}"
```

After this `https://dist.apache.org/repos/dist/release/wayang` should only contain the latest release directory.

## Releasing the Maven artifacts

Probably the simplest part is releasing the Maven artifacts.

In order to do this, the release manager logs into Nexus at `https://repository.apache.org/`, selects the staging repository and clicks on the Release button.

This will move all artifacts into the Apache release repository and delete the staging repository after that.

All release artifacts released to the Apache release repo, will automatically be synced to Maven central.


## Add the version to the DOAP file
Now that the release is out, in the develop branch, update the DOAP file for Wayang.

This is found at: 

* `src/site/resources-filtered/wayang-doap.rdf`

Please add the just released version to the top of the versions.

This file is needed for Apache’s tooling to automatically keep track of project release activity, and we use this internally too to automatically update the documentation to always reference the latest released version automatically.

## Releasing the Maven artifacts

Probably the simplest part is releasing the Maven artifacts.

In order to do this, the release manager logs into Nexus at `https://repository.apache.org/`, selects the staging repository and clicks on the Release button.

This will move all artifacts into the Apache release repository and delete the staging repository after that.

All release artifacts released to the Apache release repo, will automatically be synced to Maven central.

## Merge back release version to release branch

The release branch should always point to the last released version. This has to be done with git

```shell
git checkout release
```
```shell
git merge v0.6.0
```
When there are conflicts it could help to use the theirs merge strategy, i.e.,
```shell
git merge -X theirs v0.6.0
```
  Possibly a manual conflict resolution has to be done afterwards. After that, changes need to be pushed.

## Updating Jira

Set the released version to "released" and set the "release-date"

Add the next version to the versions.
