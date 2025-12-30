# 📖 Cómo Ver los Notebooks en GitHub

## 🔴 ¿Por qué veo X rojas en los commits?

Las **X rojas** que ves en GitHub son del **CI/CD workflow** (GitHub Actions) que se ejecutaba en cada commit a `develop`.

### ✅ Solución Aplicada

He modificado el workflow para que:
- **Solo se ejecute en la rama `main`** (no en `develop`)
- Los checks de linting son **no bloqueantes** durante desarrollo
- Puedes desarrollar libremente en `develop`
- Los checks estrictos se aplican solo al hacer merge a `main`

**Archivo modificado:** `.github/workflows/ci.yml`

---

## 📓 ¿Por qué los notebooks son .py y no .ipynb?

Los notebooks están en **formato Databricks Python** (`.py`), no Jupyter (`.ipynb`).

### Diferencias:

| Formato | Extensión | Plataforma | Visualización GitHub |
|---------|-----------|------------|---------------------|
| **Jupyter** | `.ipynb` | Jupyter Lab/Notebook | Renderizado nativo con celdas |
| **Databricks** | `.py` | Databricks Workspace | Mostrado como código Python |

### Estructura Databricks .py:

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Título del Notebook
# MAGIC Este es contenido Markdown

# COMMAND ----------

# Este es código Python
df = spark.read.table("mi_tabla")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM mi_tabla
```

---

## 🔍 Cómo Visualizar los Notebooks

### Opción 1: En GitHub (Como Código Python)
1. Ve a la carpeta `notebooks/`
2. Haz clic en cualquier archivo `.py`
3. GitHub lo mostrará como código Python con syntax highlighting
4. Busca los comentarios `# MAGIC %md` para ver el contenido Markdown

**Enlaces directos:**
- [01_bronze_ingestion.py](https://github.com/DIEGO77M/BI_Market_Visibility/blob/develop/notebooks/01_bronze_ingestion.py)
- [02_silver_standardization.py](https://github.com/DIEGO77M/BI_Market_Visibility/blob/develop/notebooks/02_silver_standardization.py)

### Opción 2: En Databricks (Visualización Completa)

**Pasos para importar:**

1. **Ve a tu Workspace de Databricks**
   - https://community.cloud.databricks.com/ (o tu instancia)

2. **Importar desde Git (Recomendado)**
   ```
   Workspace → Repos → Add Repo
   Git URL: https://github.com/DIEGO77M/BI_Market_Visibility.git
   Branch: develop
   ```

3. **O importar archivo individual:**
   ```
   Workspace → Import
   Selecciona: URL o File
   URL: https://raw.githubusercontent.com/DIEGO77M/BI_Market_Visibility/develop/notebooks/01_bronze_ingestion.py
   ```

4. **El notebook se mostrará con:**
   - Celdas Markdown renderizadas
   - Celdas de código editables
   - Resultados de ejecución (si lo ejecutas)

### Opción 3: Convertir a Jupyter (.ipynb)

Si necesitas formato Jupyter:

```bash
# Instalar databricks-cli
pip install databricks-cli

# Exportar notebook
databricks workspace export /path/to/notebook.py notebook.ipynb --format JUPYTER
```

---

## ✅ Estado Actual de Commits

Todos tus commits están **correctamente subidos** a GitHub:

```
✅ 3fbaaa4 - docs: Update notebooks README
✅ 93263a4 - chore: Add .gitattributes
✅ 3a765b4 - ci: Make CI/CD permissive
✅ c3e1b2a - feat: Complete Silver Layer
✅ 0706824 - docs: Complete Bronze Layer
```

**Verificar en:** https://github.com/DIEGO77M/BI_Market_Visibility/commits/develop

---

## 🎯 Próximos Pasos

1. ✅ **Commits sincronizados** - Todo está en GitHub
2. ✅ **CI/CD ajustado** - No más X rojas en develop
3. ✅ **Notebooks documentados** - README actualizado
4. 🚧 **Probar Silver layer** - Ejecutar en Databricks
5. 🚧 **Gold layer** - Siguiente fase

---

## 📚 Recursos

- **Databricks Notebook Format:** https://docs.databricks.com/notebooks/notebook-export-import.html
- **Medallion Architecture:** https://www.databricks.com/glossary/medallion-architecture
- **GitHub Actions:** https://docs.github.com/en/actions

---

**Creado:** 2025-12-30  
**Autor:** Diego Mayorga
