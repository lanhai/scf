---
name: scf-framework-publish
description: 当用户明确要求“发布框架”时使用。这个 skill 会严格执行框架发布的固定顺序：framework build、framework push、build_main.sh，并在出现瞬时失败时继续重试，不能跳步也不能换顺序。
---

# SCF 框架发布

## 概览

这个 skill 只用于“发布框架”这一类任务，不用于普通应用发布。

## 固定顺序

严格按下面顺序执行：

1. `framework build`
2. `framework push`
3. `/Users/lanhai/projects/swoole/etc/build_main.sh`

## 重试规则

如果某一步是瞬时失败：

1. 只重试当前这一步
2. 等它成功
3. 成功后再继续下一步

不能跳步，不能换顺序，也不能合并步骤。

## 执行前说明

真正执行前，先把将要使用的完整三步顺序重新说清楚。

如果“发布框架”只是更大发布任务的一部分，可以和 `$scf-release-publish` 一起用。

具体实现入口请看 `references/sequence.md`。
