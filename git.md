# scripts

## 1. 创建一个新功能分支（feature）
```bash
git checkout -b develop
git pull origin develop          # 保持最新

git checkout -b feature/dibo_init    # 你的分支名示例
# 或者用连字符避免潜在问题：feature-dibo-init

# 开发代码...
git add .
git commit -m "feat: 初始化项目结构和基本配置"

git push -u origin feature/dibo_init   # 推到远程，方便 PR
```

## 2. 完成 feature（合并回 develop）
```bash
# 先切回 develop
git checkout develop
git pull origin develop

# 合并（推荐 --no-ff 保持历史清晰）
git merge --no-ff feature/dibo_init
git push origin develop

# 删除临时分支（本地 + 远程）
git branch -d feature/dibo_init
git push origin --delete feature/dibo_init
```

## 3. 创建发布分支（release）
```bash
git checkout develop
git pull

git checkout -b release/1.0.0    # 版本号根据实际情况
# 在这里：修 bug、改版本号、更新文档、跑测试

git commit -m "chore: prepare release 1.0.0"

# 完成后合并到 main 并打 tag
git checkout main
git pull
git merge --no-ff release/1.0.0
git tag -a v1.0.0 -m "Release version 1.0.0"
git push origin main --tags

# 同时合并回 develop（带回 release 阶段的修复）
git checkout develop
git merge --no-ff release/1.0.0
git push origin develop

# 删除 release 分支
git branch -d release/1.0.0
git push origin --delete release/1.0.0
```

## 4. 创建紧急修复分支（hotfix）
```bash
git checkout main
git pull .

git checkout -b hotfix/1.0.1     # 修复线上 bug

# 修复代码...
git commit -m "fix: 修复登录崩溃问题"

# 合并到 main + 打新 tag
git checkout main
git merge --no-ff hotfix/1.0.1
git tag -a v1.0.1 -m "Hotfix 1.0.1"
git push origin main --tags

# 合并回 develop
git checkout develop
git merge --no-ff hotfix/1.0.1
git push origin develop

# 删除
git branch -d hotfix/1.0.1
git push origin --delete hotfix/1.0.1
```


# 删除操作
##  删除分支
```bash
# 删除本地 feature 分支
#  -d：安全删除（只有当分支已合并到当前分支时才允许删）。
git branch -d feature

# 删除本地 feature 分支
# 如果 Git 提示 “not fully merged”（分支内容还没合并），但你确定要删，就强制删除：bash
git branch -D feature    # 大写 D，强制删，不管是否合并


# 删除远程 feature 分支
# 如果这个分支也推到远程 GitHub 了，同时删除远程的bash
git push origin --delete feature
# 或简写
git push origin :feature

```

## git commands
### pull
``` bash
git pull origin main
# 它实际上等于两个命令的组合：
git fetch origin main  # 下载远程代码
git merge origin/main  # 合并到当前分支
```
### checkout
```bash
git checkout develop        # 切换到 develop 分支
git checkout -b feature     # 创建并切换到新分支 feature
git checkout 01d5188        # 切换到 01d5188 分支
```
### rm
```bash
# 从 Git 中删除但保留本地文件
git rm -r --cached .idea/
# 提交更改
git add .gitignore
git commit -m "Add .idea/ to .gitignore"
git push origin main
```
### merge
```bash
# 合并（推荐 --no-ff 保持历史清晰）
git merge --no-ff feature/dibo_init
# 先理解 fast-forward（快进合并，默认行为）
git merge feature/dibo_init

```
### push
```bash
git push -u origin feature/dibo_init   
git push origin feature/dibo_init
# -u（--set-upstream）
#推送 + 设置 upstream（把本地分支和远程分支关联起来）
# git branch --set-upstream-to=origin/feature/dibo_init feature/dibo_init
# 相当于把本地分支的“上游”设置为远程的对应分支
# 设置完成后，Git 就“记住”了这个分支应该推送到哪里、拉取自哪里

# 第一次推送新分支时：永远用 -u 

#  如何查看当前分支是否设置了 upstream？bash
git branch -vv

git push origin feature/dibo_init #：只推一次，不建立长期关系  
git push -u origin feature/dibo_init #：推 + 建立长期跟踪关系，让后续 push/pull 变得更省事
```



# docs
```
https://nvie.com/posts/a-successful-git-branching-model/
https://www.atlassian.com/git/tutorials/learn-git-with-bitbucket-cloud
```