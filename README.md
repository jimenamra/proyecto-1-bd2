# Proyecto 1 de BD2

Gestor de base de datos personalizado

1. Activar entorno virtual
```shell
conda create -n bd2_env python=3.10 -y
```

```shell
conda activate bd2_env
```

2. Levantar API backend

```shell
cd backend
```
Desplegar API local

```shell
uvicorn backend.main:app --reload
```

3. Levantar app

```shell
streamlit run frontend/app.py
```

