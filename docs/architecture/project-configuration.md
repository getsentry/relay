# Project configuration

This document describes how Relay deals with project configurations.



## Overview

## Getting a project

Here is how we obtain a Project actor ( used to get project configuration).

**Legend**

* Redis Proj Cache = Redis Project Cache
* Upstream Source = Upstream Project Source

```mermaid
sequenceDiagram

participant extern as Some Actor
participant projCache as Project Cache
participant proj as Project
participant redis as Redis Proj Cache
participant upstream as Upstream Source

Note over extern, upstream : Getting a Project returns a Project actor from the cache or simply creates a new Project actor and returns it
extern->>projCache: GetProject(projId)
activate projCache
  alt projId in ProjectCache
    projCache-->>extern: Project
  else projeId not found in cache
    projCache->>projCache: Project::new(projId, now)
    projCache-->>extern: Project
  end
deactivate projCache

```

## Getting a project state

Here is how we obtain project configuration.

**Legend**

* Proj Cache = Project Cache
* Proj Loc Info = Project Local Info
* Redis Proj Cache = Redis Project Cache
* Upstream Source = Upstream Project Source

``` mermaid
sequenceDiagram
participant extern as Some Actor
participant projCache as Proj Cache
participant local as Proj Loc Info
participant proj as Project
participant redis as Redis Proj Cache
participant upstream as Upstream Source

Note over extern, upstream : Fetching a Project state
extern->>projCache: FetchProjectState(projId)
activate projCache
  opt proj in cache
    Note right of projCache: Update last used
    projCache->>projCache: proj.last_update = now
  end
  opt projectLocal != None
    projCache ->> local: FetchOptionalProjectState(projId)
    activate local
      local -->> projCache: Option<ProjectState>
    deactivate local
  end
  alt projState != None
  	projCache -->>extern: ProjectState
  else projState == None
  	opt has redis cache
      projCache ->> redis: FetchOptionalProjectState(projId)
      activate redis

  		redis -->> projCache: Option<ProjectState>
  		deactivate redis
  	end
  	
  	alt projState != None
  		projCache -->> extern: ProjectState
  	else projState == None
  		projCache ->> upstream: FetchProjectState(projId)
  		activate upstream
  		upstream -->> extern: ProjectState
  		deactivate upstream
  	end
  end
deactivate projCache
```

## Fetching the project state from Redis

Fetching the project state from redis is straight forward.

Relay does not do any sort of management of project states in Redis. From Relay's point of view a project state is either in Redis (and then it uses it as a result) or it isn't and then it looks for the project state in other places (upstream).

If Relay obtains the project state from Upstream it will **NOT** insert it in Redis. It is up to other, external systems, to manage project states and add/remove/refresh them in Redis.

## Fetching the project state from Upstream 

If everything else fails and the ProjectCache can't obtain a project state from one of the chaches the Upstream will be queried for the ProjectState.

Here's what happens in the Upstream actor

**Legend**

* State Channel - Project State Channel
* Upstream Source - Upstream Project Source

```mermaid
sequenceDiagram
participant extern as Some Actor
participant upstream as Upstream Source
participant timer as Timer
participant channel as State Channel
participant http as Upstream Source

extern->>upstream: FetchProjectState(projId)
activate extern
  activate upstream
    upstream-->>timer: schedule_fetch()
    activate timer
    note right of upstream: Will return the <br> receiver of a <br> Project Channel
    upstream->>upstream: getOrCreateChannel(projId)
    note right of timer: at regular intervals
    timer->>upstream: fetch_states()
    deactivate timer
    activate upstream
    loop batch requests

        upstream->>http: GetProjectStates()
        activate http
          http->>upstream: GetProjectStatesResponse
        deactivate http
    end
    loop project states
      upstream->>channel: send(ProjectState)
      activate channel
    deactivate upstream
    end
  deactivate upstream
      channel->>extern: ProjectState
      deactivate channel
deactivate extern
```

.
