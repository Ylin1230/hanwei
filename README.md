# 销售工作台

## 项目结构

```
├── dashboard.html          # 前端页面（挂到百数云外链）
└── backend/
    ├── main.py             # FastAPI 入口
    ├── .env                # ⚠️ 敏感配置（不要提交 Git）
    ├── .env.example        # 配置示例
    ├── requirements.txt
    ├── routes/
    │   └── dashboard.py    # API 路由
    └── services/
        ├── config.py       # 从 .env 读取配置
        └── dashboard.py    # 百数云数据查询逻辑
```

## 安全说明

- **API Key 只存在于后端 `.env` 文件中**，前端代码里没有任何密钥
- `.env` 文件已加入 `.gitignore`，不会被提交到 GitHub
- 前端 `dashboard.html` 只请求本机后端 `http://localhost:8765/api/dashboard`

## 启动后端

```bash
cd backend

# 1. 复制并填写配置
cp .env.example .env
# 编辑 .env，填入真实 API Key

# 2. 安装依赖
pip install -r requirements.txt

# 3. 启动服务
python main.py
# 服务运行在 http://localhost:8765
```

## 部署说明

若需要将 `dashboard.html` 挂到百数云外链（GitHub Pages），后端需部署到公网服务器，
并将 `dashboard.html` 中的 `API_BASE` 改为服务器地址，例如：

```js
const API_BASE = 'https://your-server.com';
```

推荐部署平台：自有服务器（宝塔面板）、Railway、Render 等。
