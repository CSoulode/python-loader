# 项目说明：M3 DataLoader / python-loader（以当前 `feature/thesis_optimizations` 工作区实现为准）

> 目标：回答“这个仓库现在**到底在做什么**？”并把 README 里容易误导/过时的地方讲清楚。  
> 说明范围：以本仓库当前检出分支 `feature/thesis_optimizations` 的实现为准；本文已按当前 workspace 代码重新核对。
> 工作区结构说明：当前 M3 workspace 采用**并列多仓库**布局，`python-loader/`、`MetaDataCube-Client_2024/`、`vectorkv/` 都位于工作区根目录；后两者不是 `python-loader/` 的子目录。下文提到它们时，均指工作区同级仓库。

---

## 1. 一句话概括（TL;DR）

这是一个面向 **M3 Multi‑Dimensional Data Model（多维数据模型）** 的后端/工具仓库：

- 用 **PostgreSQL** 存储“媒体对象（media）+ 多维标签（tags/tagsets/taggings）+ 层级结构（hierarchies/nodes）”
- 提供一个 **Go 实现的 gRPC 服务器**（并带一个 **HTTP/REST gateway**），让客户端能：
  - CRUD：媒体、标签、标签集、层级、节点、打标关系
  - 查询：尤其是为 “Metadata Cube/浏览器” UI 计算 **Browsing State（立方体 cell 状态）**
-（可选）提供一套基于 **RabbitMQ** 的“插件”处理管线：EXIF/时间/地点/识别/描述等插件自动生成标签并写回 DB
-（历史遗留）还保留了 Python 侧的 CLI/服务器代码，但与当前 proto/Go 服务器的对齐状态需要留意（见文末“已知不一致”）

---

## 2. 运行时架构：你实际在跑哪些东西？

最常见的完整部署形态（概念上）是：

```
           +-------------------+
           |  Frontend / UI    |
           | (e.g. MetadataCube|
           +---------+---------+
                     |
                     | HTTP (REST) / gRPC
                     v
           +-------------------+             +-------------------+
           | Go DataLoader     |             | VectorKV (vectorkv)|
           | go-server/server  |             | kvserver (VectorKV)|
           | - gRPC :SV_PORT   |             | - NN/KNN/Put/Get   |
           | - REST :HTTP_PORT |             | - multi-model      |
           +---------+---------+             +---------+---------+
                     | SQL                            | SQL
                     v                                v
           +-------------------+
           | PostgreSQL        |
           | ddl.sql schema    |
           | views.sql helpers |
           | <model>_tags vec  |
           +-------------------+

  (optional, for enrichment)

           +-------------------+        +-------------------+
           | RabbitMQ (topic)  |<------>| Plugins           |
           | exchangeName env  |        | go-server/plugins |
           +---------+---------+        +---------+---------+
                     ^                            |
                     |                            | (optional)
                     |                            v
                     |                  +-------------------+
                     |                  | Media Downloader  |
                     |                  | go-server/media_  |
                     |                  | downloader (gRPC) |
                     |                  +-------------------+
```

> 上图画的是**运行时服务拓扑**，不是代码目录树。在当前 workspace 中，前端 `MetaDataCube-Client_2024/` 与向量服务 `vectorkv/` 都是 `python-loader/` 的同级仓库。

要点：

- **Go DataLoader** 是“核心服务”。它直接连 Postgres，通过 gRPC/HTTP 对外提供接口。
- **VectorKV（工作区同级仓库 `vectorkv/`）** 是“向量存储 + ANN 查询”的独立服务/工具（本阶段已实现）。它与 go-server **共用同一个 DB/schema**，并把 embedding 写入 M³ 的 `tags/<model>_tags/taggings` 链路，NN/KNN 返回 `medias.id`。
- 需要特别说明：**当前 `python-loader/go-server` 已经包含直连 `vectorkv` 的 gRPC client 集成**。相关逻辑主要落在 `go-server/server/vector_filter.go`、`vector_dimension.go`、`vector_dimension_strategy.go` 与 `server.go`：它会通过 `VECTORKV_ADDR` 建立 gRPC 连接，消费 `KNN` / `FilteredKNN` / `RangeSearch` / `FilteredRangeSearch` / `ListModels`，并把结果注入 browsing-state 管道。
- **RabbitMQ + Plugins + Media Downloader** 是“可选的自动标注/信息提取管线”。它们让系统能在新媒体进入后自动提取 EXIF、生成 caption、分类、做人脸识别等，然后把结果作为 taggings 写回数据库。
- 当前分支里 RabbitMQ 相关代码在 `go-server/server/server.go` 的 `main()` 内有注释掉的部分（见后文“插件管线现状”），意味着：**代码具备插件机制，但是否启用取决于你怎么部署/是否把那段启用逻辑打开**。

---

## 3. 核心数据模型（PostgreSQL）

### 3.1 `ddl.sql`：基础表与约束

`ddl.sql` 定义了核心实体（简化）：

- `medias`：媒体对象（URI、类型、缩略图 URI）
- `tag_types`：标签类型（基础 `ddl.sql` 固定 5 种；向量功能会额外确保存在 `description='vector'`）
- `tagsets`：标签集（维度/字段的集合，例如“Location”“Timestamp UTC”）
- `tags`：标签主表（指向 tagset + tagtype）
- `*_tags`：按类型拆表存实际值
  - `alphanumerical_tags`
  - `timestamp_tags`
  - `time_tags`
  - `date_tags`
  - `numerical_tags`
- `taggings`：media ↔ tag 的多对多关系（一个 media 可以有多个 tag，一个 tag 可以挂多个 media）
- `hierarchies`：层级（通常挂在某个 tagset 上）
- `nodes`：层级节点（每个 node 关联一个 tag；node 之间用 parentnode_id 形成树）

其中 `tag_types` 的 id 映射在 `ddl.sql` 末尾插入（前 5 种固定）：

1. `alphanumerical`
2. `timestamp`
3. `time`
4. `date`
5. `numerical`

向量功能会额外需要：

6. `vector`（通常插入后会成为 id=6，但实现不依赖固定 id；运行时按 `description='vector'` 查询）

并且有触发器 `check_matching_tagtype`：保证 `tags.tagtype_id` 与 `tagsets.tagtype_id` 一致。

### 3.2 `views.sql`：Browsing State 相关的函数 + 物化视图（重要）

Browsing State（立方体 cell 计算）相关的 SQL 生成器会用到：

- SQL 函数：
  - `get_subtree_from_parent_node(int)`
  - `get_level_from_parent_node(int, int)`
  - `get_level_from_sibling(int, int)`
- 物化视图（materialized view）：
  - `nodes_taggings`：把“节点子树 → tag → object”展开
  - `tagsets_taggings`：把“tagset → tag → object”展开

这些内容来自 `views.sql`，通常需要在建表后执行一次（并在数据变化较大时考虑刷新物化视图；例如用 `kvload` 批量导入 embedding 后，若 browsing-state 查询走 `tagsets_taggings`，需要 `REFRESH MATERIALIZED VIEW tagsets_taggings;`）。

如果你只执行了 `ddl.sql` 而没有执行 `views.sql`，Go 服务器里某些 browsing state 查询会直接报 “relation does not exist” 或性能非常差。

---

## 4. 这个项目最“核心”的能力：Browsing State（立方体 Cell 状态）是什么？

在面向 UI（类似 MetadataCube）的场景里，用户会：

1. 选择三条轴（X/Y/Z）来“切片”数据集  
   - 每条轴要么是一个 `tagset`（例如“Location”），要么是某棵 `hierarchy` 的某个 `node`（例如“Dates”树下的某个父节点）
2. 选择若干过滤条件（filters），进一步缩小对象集合
3. 系统返回一个三维网格中每个 cell 的结果：
   - 这个 cell 下匹配对象的 `count`
   - 以及一个/多个代表性的 `CubeObject`（当前实现通常是代表 1 个对象 + 计数）

在 `protos/dataloader.proto` 中，对应响应是 `BrowsingStateResponse`：

- `x/y/z`：**轴上的位置索引**（注意不是数据库 ID）
- `count`：该 cell 的对象数
- `cubeObjects`：代表对象列表（含 `id/fileUri/thumbnailUri`）

为什么 `x/y/z` 是“位置索引”？  
因为轴的成员是“有序列表”（例如 tagset 下的 tags 按名字排序、node 的子节点按名字排序）。服务器会先把“成员 ID → 位置”映射初始化出来，然后把 SQL 结果里的成员 ID 映射为位置编号（从 1 开始）。

对应的初始化逻辑在：

- `go-server/server/querygen/query_types.go`：`ParsedAxis`
- `go-server/server/querygen/query_generator.go`：`BuildInitializeIdsPlan()`
- `go-server/server/browsing_state_calls.go`：`initXYZAxes()` + `ExecuteInitializeIdsPlan()`

---

## 5. 本仓库结构速览：每个目录/文件干什么？

- `protos/dataloader.proto`：当前（Go 侧使用的）gRPC API 定义 + 部分 HTTP 注解
- `ddl.sql`：数据库 schema（基础表/约束/触发器）
- `views.sql`：browsing state/层级查询相关 SQL 函数 + 物化视图 + 索引
- `go-server/`：Go 侧实现（当前分支重点）
  - `go-server/server/`：DataLoader gRPC 服务 + REST gateway + browsing state 计算
  - `go-server/server/querygen/`：生成 browsing state 相关 SQL
  - `go-server/rabbitMQ/`：RabbitMQ producer/consumer 辅助代码（Go + Python）
  - `go-server/media_downloader/`：独立的 gRPC 服务，下载/缓存远程媒体供插件使用
  - `go-server/plugins/`：可选插件（EXIF、时间派生、地点派生、captioning、分类、人脸识别、日期层级构建…）
  - `go-server/build.sh`：构建 go-server 二进制到 `go-server/out/server`
- `client/`：Python CLI（Click）+ gRPC client（用于导入/导出/测试）
- `server/`：Python gRPC server（历史实现；与当前 proto/Go 侧对齐状态需确认）
- `docker-compose.yaml`：尝试一键拉起 db/rabbitmq/go-server/plugins 的 compose（但当前仓库快照里存在不一致点，见后文）
- `.devcontainer/`：Go 服务器的 devcontainer 开发环境（方便在容器里编译/调试）

和本仓库经常一起联调、但**不属于本仓库**的工作区同级目录还有：

- `MetaDataCube-Client_2024/`：Angular Web 客户端；本仓库通过 HTTP 兼容层为它提供旧 C# 风格的 REST JSON
- `vectorkv/`：向量存储与 ANN 查询服务（Go + gRPC + Postgres + pgvector）
  - `vectorkv/cmd/kvserver`：VectorKV gRPC 服务（多模型；把 embedding 写入 `tags/<model>_tags/taggings`）
  - `vectorkv/cmd/kvload`：离线导入工具（`file_uri -> medias.id` 映射后批量写入）
  - `vectorkv/cmd/kvmigrate`：schema ensure（pgvector + vector tag_type + tagset + 向量表 + 索引）
- 工作区根目录 `docs/architecture/overview.md` / `docs/architecture/vector_design.md`：跨仓库架构与向量设计说明

---

## 6. API：对外接口有哪些？（gRPC + REST）

### 6.1 gRPC（以 `protos/dataloader.proto` 为准）

服务名：`DataLoader`

主要类别：

- Medias：`getMedias/getMediaById/getMediaByURI/createMedia/createMediaStream/deleteMedia`
- TagSets：`getTagSets/getTagSetById/getTagSetsById/getTagSetByName/createTagSet`
- Tags：`getTags/getTag/createTag/createTagStream/changeTagName`
- Taggings：`getTaggings/getMediasWithTag/getMediaTags/createTagging/createTaggingStream/changeTagging`
- Hierarchies：`getHierarchies/getHierarchy/createHierarchy`
- Nodes：`getNodes/getNode/getChildNodes/createNode/createNodeStream/deleteNode`
- Browsing State：`getCell(兼容)/getBrowsingState/getBrowsingState2/...`（多个实验/优化变体）
- Database：`resetDatabase`

#### Go 服务器实现覆盖情况（重要：不是所有 RPC 都真的实现了）

在 `go-server/server/*.go` 中可以看到已实现的方法大致包括：

- ✅ medias：`GetMedias/GetMediaById/GetMediaByURI/CreateMedia/DeleteMedia`
- ✅ tagsets：`GetTagSets/GetTagSetById/GetTagSetsById/GetTagSetByName/CreateTagSet`
- ✅ tags：`GetTags/GetTag/CreateTag/ChangeTagName`
- ✅ taggings：`GetTaggings/GetMediasWithTag/GetMediaTags/CreateTagging/CreateTaggingStream/ChangeTagging`
- ✅ hierarchies：`GetHierarchies/GetHierarchy/CreateHierarchy`
- ✅ nodes：`GetNodes/GetNode/CreateNode`
- ✅ browsing state：`GetBrowsingState/GetBrowsingState2/GetBrowsingStateNonDistinctBranchesIncrementalGrouping/...`（多种）
- ✅ reset：`ResetDatabase`

同时也存在“proto 里有，但当前 Go 服务器还没补齐/可能不可用”的点（建议当成已知 TODO）：

- ⚠️ `getChildNodes`：gRPC 方法当前未实现（但 REST 兼容层已提供 `/api/node/{id}/children`，见下文）
- ⚠️ `getCell`：proto 里用于旧 HTTP 兼容的 RPC，Go 侧未实现；HTTP 走自定义 handler（见下）

### 6.2 REST 兼容层（为工作区同级客户端 `MetaDataCube-Client_2024` 提供 C# 服务器风格的 JSON）

背景：Angular 客户端 `MetaDataCube-Client_2024` 默认对接的是 C# 服务器，它依赖一组固定的 REST 路径和字段命名（例如 `cubeObjects[0].fileURI` / `thumbnailURI`），而 Go 侧的 gRPC‑gateway/`protojson(UseProtoNames=true)` 输出经常是 `snake_case` 或 streaming wrapper，不直接兼容。

为此，Go 服务器现在在 HTTP mux 上加了一层“兼容 handler”，代码在：

- `go-server/server/metadata_cube_compat_http.go`
- 路由注册在 `go-server/server/server.go`

兼容层提供的主要端点与返回结构：

- `GET /api/tagset` → `[{ id, name, tagTypeId }]`
- `GET /api/tagset/{id}` → `{ id, name, tags: [{ id, name, tagsetId }], hierarchies: [{ id, name, tagsetId, rootNodeId }] }`
- `GET /api/hierarchy/{id}` → `{ id, name, tagsetId, rootNodeId, nodes: [ { id, tag: { id, name }, children: [...] } ] }`
- `GET /api/node/{id}/Children`（以及 `/api/node/{id}/children`）→ `[{ id, name }]`（顺序与 browsing state 坐标映射一致）
- `GET /api/cell` / `GET /api/cell/`
  - 当带 `all`（客户端使用 `all=[]`）→ 直接返回媒体列表：`[{ id, fileURI, thumbnailURI }]`
  - 不带 `all` → 返回 browsing state cells：`[{ x, y, z, count, cubeObjects: [{ id, fileURI, thumbnailURI }] }]`
- `GET /api/cubeobject/{id}/tags` → `[{ tagsetName, name }]`

其中 `/api/cell` 的 browsing state（不带 `all`）会直接使用旧 JSON 过滤器格式（`filters=[{type,ids,ranges}]`）生成 SQL，从而保留客户端所需的语义：

- `filters` 数组的不同元素之间是 **AND**
- 同一个元素内的 `ids[]` 是 **OR**

这点对“同一类型（tag/node）但不同 tagset 的多个过滤器”很关键（客户端会把它们拆成多个元素）。
- ⚠️ `createTagStream`：Go 侧未实现
- ⚠️ `createMediaStream`：Go 侧有批量插入实现，但方法名是 `CreateMedias`，并不会覆盖 gRPC 的 `CreateMediaStream`（因此对外仍可能是 Unimplemented）
- ⚠️ `deleteNode`：返回 Unimplemented

> 上面这些不影响“/api/cell browsing state 优化”这一分支的核心目标，但会影响“把 Go server 当完整 loader 来用”。

### 6.3 REST（HTTP gateway + 自定义兼容层）

Go 服务器在 `go-server/server/server.go` 同时启动：

- gRPC：`SV_HOST:SV_PORT`（默认 50051）
- REST gateway：监听 `HTTP_PORT`（默认 8080）

#### Go server 必需环境变量（不设置会直接退出）

`go-server/server/server.go` 在启动时会读取以下环境变量（缺失会 `log.Fatalf`）：

- 数据库连接：`DB_NAME` / `DB_USER` / `DB_PASSWORD` / `DB_HOST` / `DB_PORT`
- gRPC 监听：`SV_HOST` / `SV_PORT`
- HTTP 监听：`HTTP_PORT`
- 批量写入相关：`BATCH_SIZE`
- RabbitMQ 连接：`RABBITMQ_HOST` / `RABBITMQ_PORT` / `RABBITMQ_USER` / `RABBITMQ_PASS` / `RABBITMQ_EXCHANGE_NAME`
- browsing state 布尔开关：`UNGROUPED_QUERY_DISABLE_HASH_JOIN` / `UNGROUPED_QUERY_FORCE_JOIN_ORDERS` / `UNGROUPED_QUERY_USE_LATERAL_MEDIA_JOIN`

后两类很容易被忽略，但按当前代码它们同样是**启动期必需项**：

- `RABBITMQ_*` 虽然在 `main()` 里暂时没有启用 producer/consumer 初始化，但 `rabbitMQ` 包的包级变量在进程启动时就会读取它们。
- `UNGROUPED_QUERY_*` 虽然语义上是 feature flag，但 `browsing_state_calls.go` 当前也是通过 `MustGetEnv(...)` 读取，因此至少要显式给出 `0` 或 `1`。

另外，调试相关的可选环境变量还有：

- `SQL_TRACE` / `SQL_TRACE_PATH` / `SQL_TRACE_STDOUT`（用于把生成 SQL 打到文件/控制台）

当前实际生效的 REST 兼容层，不是旧的 `http_func_overrides.go`，而是 `go-server/server/metadata_cube_compat_http.go` 中那组 handler；`server.go` 注册的也是这套实现。它们的特点是：

- `GET /api/tagset`
  - 参数：`tagTypeId`（可选）
  - 当前实现：直接查数据库，返回兼容旧客户端字段名的 JSON 数组
- `GET /api/tagset/{id}`
  - 当前实现：直接查数据库并组装 tags/hierarchies 明细
  - 这也绕开了 gRPC `GetTagSetsById` 对 `tagset_tags` / `tagset_hierarchies` 视图的依赖
- `GET /api/node/{id}/children`（以及 `/api/node/{id}/Children`）
  - 当前实现：直接查数据库
  - **不依赖** gRPC `GetChildNodes`，因此不受该 RPC 未实现的影响
- `GET /api/cell` 与 `GET /api/cell/`
  - 这是当前兼容旧客户端的核心入口
  - 内部会复用旧格式参数解析、`initXYZAxes()` 和 query generator，但**直接执行 SQL 并返回 JSON**，不是先走 gRPC `GetBrowsingState2`

`go-server/server/http_func_overrides.go` 里那套 “HTTP -> gRPC client -> 收集 stream -> 再输出 JSON” 的 handler 仍然保留在仓库中，但当前 `server.go` 并没有注册它们，所以它们更接近历史/备用实现，而不是线上默认路径。

#### `/api/cell` 的查询参数格式（非常关键）

`/api/cell` 目前走的是“旧格式”（兼容 C# 服务/旧客户端）：

- `xAxis` / `yAxis` / `zAxis`：一个 JSON 对象字符串，对应 `querygen.ParsedAxis`：
  - 例：`{"type":"tagset","id":12}`
  - 例：`{"type":"node","id":345}`
- `filters`：一个 JSON 数组字符串，对应 `querygen.ParsedFilter[]`：
  - 例：`[{"type":"tagset","ids":[12],"ranges":[]},{"type":"tag","ids":[99],"ranges":[]}]`
- `all`：非空时进入 “all” 模式（返回对象列表）
- `timeline`：**proto 与 gRPC browsing-state 方法支持该参数，但当前兼容 REST handler 并没有实现专门的 timeline 分支**

注意：这些 JSON 作为 query string 参数时需要 URL 编码（大多数客户端库会自动处理）。

一个“概念上”的未编码示例（实际请求请做 URL 编码）：

```
/api/cell?
  xAxis={"type":"tagset","id":1}&
  yAxis={"type":"tagset","id":2}&
  zAxis={"type":"node","id":3}&
  filters=[{"type":"tagset","ids":[4],"ranges":[]}]
```

补充：旧格式里 `type` 字段常见取值为 `tagset` / `node` / `tag`。旧的 `http_func_overrides.go` 里还兼容了 `daterange/dateRange`、`timeRange`、`timestampRange` 等字符串并映射到 proto `FilterValueType`；但当前实际生效的 `metadata_cube_compat_http.go` 路径是直接使用旧解析/SQL 逻辑，而不是再走一层 gRPC 请求转换。

#### 关于 “streaming” 的一个现实限制（REST 场景）

当前实际生效的兼容 REST handler 本身就是“直接查库 -> 在内存里组装切片 -> 一次性返回 JSON 数组”的实现，因此：

- 对 HTTP 调用方来说，它不是“边算边回”的 chunked streaming 响应
- gRPC 里那些 sender goroutine / bounded channel / 增量 flush 优化，主要只对直接走 gRPC 的调用链路生效
- 旧的 `http_func_overrides.go` 也同样不是 chunked streaming，只是它采用的是“先吃完 gRPC stream，再吐 JSON 数组”的路径

当前 `/api/cell` 兼容实现主要位于：

- `go-server/server/browsing_state_calls.go`：`oldParseAxesAndFilters()` / `oldParseAxesAndFiltersFromRequest()`
- `go-server/server/metadata_cube_compat_http.go`：`GetMetaDataCubeCompatCellHandler()`

---

## 7. Browsing State 的实现/优化（开发者 thesis 分支重点）

这部分需要区分 **gRPC 浏览状态实现** 和 **当前默认 REST `/api/cell` 路径**。

旧的开发者留言里曾把 `/api/cell -> GetBrowsingState2` 当作当前主路径，但按现在 `server.go` 的注册逻辑，这个说法已经过时了。

更准确地说：

> - gRPC 侧较新的基线路径是 `GetBrowsingState2`（性能优于 `GetBrowsingState`）  
> - 最强 streaming 变体是 `GetBrowsingStateNonDistinctBranchesIncrementalGrouping`  
> - streaming 频率/队列大小等目前主要靠 `browsing_state_calls.go` 的硬编码常量  
> - `/api/cell` 和 `/api/cell/` 都支持，但当前走的是兼容 REST 的直查数据库 handler

### 7.1 `GetBrowsingState`（基线版本）

位置：`go-server/server/browsing_state_calls.go`

特点（概念上）：

- 生成一个 SQL（通常含 GROUP BY / 计数）
- 每扫描到一行就直接 `stream.Send(resp)`
- 对 gRPC 的 `Send` 没有并发隔离/背压队列

### 7.2 `GetBrowsingState2`（gRPC 优化版本，不是当前 `/api/cell` 的默认实现路径）

位置：`go-server/server/browsing_state_calls.go`

关键改动：

- **单独的 sender goroutine** 负责调用 `stream.Send`（避免并发 Send、并更好控制吞吐）
- 业务 goroutine 扫描 DB 行后把已构造好的 `BrowsingStateResponse` 放进 **有界 channel**（提供背压，避免无限占用内存）
- 可选：在 TX 内执行 `SET LOCAL enable_hashjoin = off`（由 `UNGROUPED_QUERY_DISABLE_HASH_JOIN=1` 控制）

因此它对“扫描速度”和“发送速度”解耦，整体吞吐/稳定性更好。  
但要注意：**当前 `server.go` 注册的 `/api/cell` 兼容端点并不会调用它**；该端点现在由 `metadata_cube_compat_http.go` 直接跑 SQL。

### 7.3 `GetBrowsingStateNonDistinctBranchesIncrementalGrouping`（开发者称“最强 streaming 变体”）

位置：`go-server/server/browsing_state_calls.go`

思路（概念上）：

- SQL 端：生成 “ungrouped” 的 join（不在 DB 里 GROUP BY / DISTINCT），尽快吐出行
- Go 端：在线聚合（按 `(x,y,z)` key 累加计数、挑代表对象）
- 通过一个专门的 flusher 线程按时间片/批大小把“已更新的 cell”持续推给客户端（authoritative updates）

这种方式的优点是：

- **TTFB 更低**：更快看到第一批结果
- 适合“前端逐步填充网格”的交互体验
- 代价是：服务端需要做更多聚合/去重工作，且 streaming 参数（flush 频率、队列容量等）会影响表现

### 7.4 当前可配置项（env + 常量）

位于 `go-server/server/browsing_state_calls.go`：

- 常量（当前硬编码）：
  - `flushInterval`
  - `streamBatchSize`
  - `defaultCellMapCap` / `dirtyChanCap` 等容量参数
- 环境变量（当前代码要求显式设置，即使值只是 `0`）：
  - `UNGROUPED_QUERY_DISABLE_HASH_JOIN=1`：在 TX 内关闭 hash join
  - `UNGROUPED_QUERY_FORCE_JOIN_ORDERS=1`：启用固定 join order 相关逻辑
  - `UNGROUPED_QUERY_USE_LATERAL_MEDIA_JOIN=1`：影响 querygen 生成的 SQL（是否用 LATERAL join）
  - `SQL_TRACE/SQL_TRACE_PATH/SQL_TRACE_STDOUT`：把生成的 SQL 打到文件/stdout（用于性能分析与复现）

---

## 8. 插件管线（RabbitMQ）怎么工作？现状如何？

### 8.1 主题（topic）与消息形态

插件体系采用 RabbitMQ 的 topic exchange（`RABBITMQ_EXCHANGE_NAME`）：

- 插件侧常见的输出 topic：
  - `tagging.not_added.<tagTypeId>.<tagsetName>`
    - body：JSON（字符串），通常为 `{"taggingValue":"...","mediaID":"..."}`  
    - 语义：插件算出了一个“应当存在的 tagging”，但不确定 DB 是否已有；交由 DataLoader 落库（幂等处理）
  - `hierarchy`
    - body：JSON（字符串），描述要创建的层级/节点链

Go server 侧有对应消费者逻辑：

- `listenForTaggingMessage()`：订阅 `tagging.not_added.*.*` 并落库（创建 tagset/tag/tagging）
- `listenForHierarchyMessage()`：订阅 `hierarchy` 并创建 hierarchy/root node/child nodes

位置：`go-server/server/server.go`

### 8.2 ⚠️ 现状：Go server 的 RabbitMQ 启动逻辑当前被注释

在 `go-server/server/server.go` 的 `main()` 里可以看到：

- producer 初始化（`prod = rmq.ProducerConnexionInit()`）目前被注释
- `listenForTaggingMessage()` / `listenForHierarchyMessage()` 的 goroutine 启动也被注释

这意味着：如果你按现状启动 Go server，插件即便跑起来并发布消息，也可能 **没有消费者把结果写回数据库**。

此外：部分 RPC（例如 `ChangeTagName`、`ChangeTagging`）会无条件调用 `rmq.PublishMessage(prod, ...)`，如果 `prod` 没初始化会有 nil 指针风险 —— 因此“无 RabbitMQ 模式”下要避免调用这些 RPC，或自行做保护/初始化。

### 8.3 插件与 media-downloader 的职责（简述）

- `go-server/media_downloader/`：把 HTTP(S) URI 下载到容器内缓存目录，提供 gRPC：
  - `RequestMedia(media_uri) -> media_path`
  - `ReleaseMedia(media_uri)`
  - 带 TTL 与 LRU/容量控制（`MAX_CACHE_SIZE`、`RESSOURCE_TTL`）
- `go-server/plugins/exif_extractor`（Python）：订阅 `media.*`，提取 EXIF 时间/GPS，发布 `tagging.not_added...`
- `go-server/plugins/time_info`（Go）：订阅 timestamp taggings，派生“星期/月份/时间段”等 taggings
- `go-server/plugins/location_info`（Go）：订阅 Location，调用 GeoNames API 反查城市/国家/POI（用户名目前硬编码）
- `go-server/plugins/date_hierarchy_builder`（Go）：根据 timestamp 发布一个日期层级（hierarchy）创建消息
- `go-server/plugins/captioning/classifier/face_recognition`（Python）：模型推理类插件（依赖更重）

---

## 9. 如何跑起来（按“现状能做什么”给建议）

### 9.1 开发 Go server：`.devcontainer/`

`.devcontainer/devcontainer.json` 提供了一个 Go 开发容器，通常用于：

- 在容器内编译/运行 `go-server/server`
- 端口转发：`50051`（gRPC）与 `8080`（REST）
- 通过 `.devcontainer/devcontainer.env` 注入 DB/Rabbit/优化相关环境变量

注意 `.devcontainer/devcontainer.json` 里有 `--network emm_cube_small_default`：这暗示 DB/RabbitMQ 可能在另一个 compose 项目/网络里（你的实际环境需要有这个 network，或者调整为你自己的）。

### 9.2 `docker-compose.yaml`（当前仓库快照里存在不一致点）

仓库根目录有 `docker-compose.yaml`，目标是拉起：

- `db`（postgres）+ 初始化 `ddl.sql`
- `go-server`
- `rabbitmq`
- `media-downloader`
- 若干 plugins

但就**当前仓库内容**而言，需要留意：

- `go-server` 与 plugins 使用的 `env_file: go-server/rabbitMQ/rabbitMQ.env` 在仓库中不存在（会导致 compose 失败或缺 env）。
- 仓库根目录虽然有 `rabbitMQ.env`，但 compose 目前引用的是另一个不存在的路径，因此现状仍然是错配。
- `go-server` 服务在 `environment:` 里未显式提供 `HTTP_PORT`，而 Go server 启动时要求该变量存在。
- `go-server` 在 compose 里只映射了 `50051:50051`，如果你要访问 REST gateway，还需要暴露 `HTTP_PORT`（默认 8080）。
- `resetDatabase` RPC 在容器内很可能找不到 `../../ddl.sql`（因为 go-server 的 Docker build context 不包含仓库根 `ddl.sql`）。

因此：当前 compose 更像是“未来可用的一键方案草稿”，需要做一次对齐/清理后才适合当成主跑法。

### 9.3 只跑 Go server（不带 RabbitMQ）的思路

开发者留言提到“希望提供一个最小 docker 容器运行 go-server（不含 RabbitMQ 功能）”。就当前代码来看，可行路径是：

- 确保设置完整的启动期环境变量：至少 `DB_*`、`SV_*`、`HTTP_PORT`、`BATCH_SIZE`、`RABBITMQ_*`、`UNGROUPED_QUERY_*`
- 如果你真的不打算启用 RabbitMQ，也仍然需要先提供一组占位的 `RABBITMQ_*` 值；否则进程会在包初始化阶段直接退出
- 仅使用 `/api/cell`（browsing state）与那些不依赖 RabbitMQ 的 RPC
- 或者把 `prod` 初始化和消费者启用做成可配置开关（目前代码还没做完这一层）

---

## 10. 分支说明（开发者留言整理）

开发者声明（摘录要点）：

- 最新工作分支是 `feature/thesis_optimizations`（尚未合并进 main）
- 未沟通前不要向该分支 push（避免影响其 thesis 写作期间的服务器运行）
- browsing state 优化已基本完成：
  - 较新的 gRPC 基线路径是 `GetBrowsingState2`（禁用 hash join、性能更好）
  - 最强 streaming 变体：`GetBrowsingStateNonDistinctBranchesIncrementalGrouping`
  - streaming 频率/队列参数目前主要通过 `go-server/server/browsing_state_calls.go` 的常量控制，计划后续改成 `.env` 可配置
- `GET /api/cell` 已按预期工作，并兼容 `/api/cell/`（多一个 `/`）；但当前注册的是兼容 REST 的直查数据库 handler，而不是 `GetBrowsingState2`

---

## 11. 当前仍需注意的历史边界

结合当前仓库内容，仍有这些容易踩坑的历史边界需要单独注意：

- Python server（`server/app.py`）使用的 `server/dataloader_pb2.py` 与当前 `protos/dataloader.proto` 的对齐状态需要重新验证，因此“Python/Go server 可互换”不能默认成立。
- Go server 的实现重心显然在 browsing state 与 HTTP 兼容层，但部分 loader/streaming RPC 仍未补齐（例如 `createMediaStream`、`createTagStream`、`getChildNodes` 等）。
- Go server 的 `GetTagSetsById` 会查询数据库视图/表 `tagset_tags`、`tagset_hierarchies`，但当前仓库提供的 `ddl.sql` 与 `views.sql` 并未创建它们；如果你要用到该接口，需要补齐相应 SQL（或改用其它查询方式）。
- `docker-compose.yaml` 与当前仓库中的 RabbitMQ/env 文件路径仍存在错配，若要用 compose 跑完整插件链，必须先逐项复核。

当前顶层 `README.md` 已按 workspace 现状刷新；涉及更细粒度实现事实时，仍以本说明文档和代码实现为准。
