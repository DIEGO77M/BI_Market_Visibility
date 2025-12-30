# CI/CD Workflow Status

## 🚧 Current Status: DISABLED

El workflow de CI/CD (`ci.yml`) ha sido **temporalmente deshabilitado** durante la fase de desarrollo activo.

**Archivo actual:** `ci.yml.disabled`

---

## ❓ ¿Por qué está deshabilitado?

Durante el desarrollo de las capas Bronze, Silver y Gold del proyecto, los checks automáticos estaban causando:

- ❌ **X rojas en todos los commits** - Checks fallando constantemente
- 🐌 **Lentitud en el desarrollo** - Esperar a que fallen los tests en cada push
- 🔧 **Tests incompletos** - El proyecto aún no tiene suite completa de tests
- 📝 **Linting estricto** - Código funcional rechazado por formateo

---

## ✅ Checks que Estaban Ejecutándose

El workflow incluía:

### 1. **Tests Unitarios**
- pytest en Python 3.8, 3.9, 3.10
- Cobertura de código
- Upload a Codecov
- **Estado:** ❌ Fallando (tests incompletos)

### 2. **Linting**
- flake8 (syntax + style)
- black (code formatting)
- isort (import sorting)
- **Estado:** ⚠️ Warnings (código no formateado)

### 3. **Security Scan**
- safety (dependencies)
- bandit (code security)
- **Estado:** ❌ Fallando (dependencias sin validar)

---

## 🔄 ¿Cuándo se Re-habilitará?

El workflow se volverá a activar cuando:

1. ✅ **Capas Bronze, Silver, Gold completadas**
2. ✅ **Suite completa de tests implementada**
3. ✅ **Código formateado con black + isort**
4. ✅ **Dependencias validadas con safety**
5. ✅ **Listo para merge a `main`**

---

## 🚀 Para Re-habilitar el Workflow

Cuando estés listo para producción:

```bash
# Renombrar de vuelta
mv .github/workflows/ci.yml.disabled .github/workflows/ci.yml

# Commit y push
git add .github/workflows/ci.yml
git commit -m "ci: Re-enable CI/CD for production release"
git push origin develop

# Merge a main
git checkout main
git merge develop
git push origin main
```

---

## 📋 Checklist Pre-Producción

Antes de re-habilitar CI/CD, completar:

- [ ] **Tests:** Implementar tests para data_quality.py, spark_helpers.py
- [ ] **Linting:** Ejecutar `black src/` y `isort src/`
- [ ] **Docs:** Actualizar README con resultados finales
- [ ] **Data Dictionary:** Completar todas las tablas Gold
- [ ] **Power BI:** Dashboard finalizado con screenshots
- [ ] **Presentation:** Ejecutiva completa

---

## 🎯 Desarrollo Actual

**Fase:** Silver Layer → Gold Layer  
**Branch:** develop  
**CI/CD:** Deshabilitado  
**Próximo Milestone:** Completar Gold layer + Star Schema

---

**Última actualización:** 2025-12-30  
**Autor:** Diego Mayorga
