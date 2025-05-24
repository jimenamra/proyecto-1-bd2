# Proyecto 1 de BD2

Gestor de base de datos personalizado

1. Activar entorno virtual (opcional)
```shell
conda create -n bd2_env python=3.10 -y
```

```shell
conda activate bd2_env
```

2. Instalar dependencias

```shell
pip install -r requirements.txt
```

3. Levantar API backend

```shell
cd backend
```
Desplegar API local

```shell
uvicorn backend.main:app --reload
```

4. Levantar app (en simultaneo con API backend)

```shell
streamlit run frontend/app.py
```

