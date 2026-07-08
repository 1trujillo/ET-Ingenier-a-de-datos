README - Cómo ejecutar los tests de la simulación de movilidad urbana
=====================================================================

Resumen
-------
Este repositorio incluye una simulación de pipeline de datos en tiempo real (ingestión → ETL → salida) que emite métricas similares a Datadog. El runner de pruebas (`test.py`) ejecuta escenarios diseñados para estresar y maximizar métricas como:

- `movilidad.accidentes_por_comuna`
- `pipeline.success_rate_pct`
- `pipeline.throughput.events_per_min`
- `etl_duration_seconds`
- `events_failed`

Archivos clave
-------------
- `main.py`  : implementación del pipeline y función `run_scenario(...)`.
- `test.py`  : runner con 4 escenarios (NORMAL LOAD, HIGH LOAD, ERROR SCENARIO, ACCIDENT SPIKE SCENARIO).

Requisitos
---------
- Python 3.8+
- Paquete opcional: `requests` (solo si quieres que las métricas se envíen a un endpoint HTTP simulado).

Instalación rápida
-----------------
Recomendado: crear un entorno virtual e instalar `requests` si lo necesita.

Windows (cmd/powershell):

```powershell
python -m venv .venv
# Windows PowerShell
.\.venv\Scripts\Activate.ps1
# O en cmd
.\.venv\Scripts\activate.bat
pip install requests
```

Linux / macOS:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install requests
```

Cómo ejecutar los tests
----------------------
Ejecutar el conjunto completo de escenarios (por defecto cada escenario se ejecuta durante el número de segundos indicado con `--duration`):

```bash
python test.py --duration 30
```

Parámetros útiles:
- `--duration`: duración en segundos de cada escenario (ej. `--duration 60`).
- `--batch-size`: tamaño del batch de ETL (ej. `--batch-size 50`).

Ejemplos:

- Ejecutar todos los escenarios durante 60 segundos cada uno:

```bash
python test.py --duration 60
```

- Ejecutar con batches más grandes (aumenta carga por batch):

```bash
python test.py --duration 30 --batch-size 50
```

Correr solo la simulación rápida desde `main.py`:

```bash
python main.py
```

Simular envío a Datadog (opcional)
---------------------------------
Para que la función `send_metric(...)` intente enviar las métricas a un endpoint HTTP (simulando Datadog), exporta la variable `DATADOG_URL` antes de ejecutar.

Windows (cmd):

```cmd
set DATADOG_URL=http://localhost:8126/v1/series
python test.py --duration 30
```

PowerShell:

```powershell
$env:DATADOG_URL = 'http://localhost:8126/v1/series'
python test.py --duration 30
```

Linux / macOS:

```bash
export DATADOG_URL="http://localhost:8126/v1/series"
python test.py --duration 30
```

Salida esperada
---------------
Durante la ejecución verás líneas de log con métricas en consola, por ejemplo:

```
[METRIC] pipeline.success_rate_pct=97.5 tags=['env:test', 'scenario:high_load']
[METRIC] pipeline.throughput.events_per_min=12000.0 tags=[...]
[METRIC] etl_duration_seconds=0.0958 tags=[...]
[METRIC] events_failed=12 tags=[...]
[METRIC] movilidad.accidentes_por_comuna=42 tags=[..., 'comuna:santiago']
```

Al final de cada escenario se imprimirá un resumen con:
- total_events
- successful_events
- failed_events
- success_rate_pct
- throughput_events_per_min
- etl_duration_seconds
- accidents_by_comuna

Cómo aumentar la carga / ajustar comportamiento
---------------------------------------------
- Subir `target_rate_per_sec` en `test.py` (la lista `scenarios` contiene pares `(label, target_rate)`), o modificar los valores para escenarios individuales.
- Aumentar `--batch-size` para procesar más eventos por batch.
- Cambiar la duración `--duration` para pruebas más largas.
- Para forzar más eventos corruptos o picos de accidentes, editar las condiciones en `_build_event` dentro de `main.py`.

Sugerencias para monitoreo y dashboard
------------------------------------
- Grafica `pipeline.throughput.events_per_min` en un eje y `etl_duration_seconds` en otro para detectar cuellos de botella.
- Usar `movilidad.accidentes_por_comuna` por comuna para crear alertas de picos locales.
- Crear alertas en `events_failed` para detectar degradación del pipeline.

Problemas comunes
-----------------
- Si no ves métricas HTTP enviadas, asegúrate de tener `DATADOG_URL` definida y que `requests` esté instalado.
- Si la CPU local se satura al aumentar `target_rate_per_sec`, reduce `batch-size` o ejecuta en una máquina más potente.

Próximos pasos (opcional)
-------------------------
- Añadir un script `requirements.txt` o `pyproject.toml` para gestionar dependencias.
- Empaquetar la simulación en un contenedor Docker para pruebas reproducibles.

Contacto
--------
Si quieres que adapte escenarios concretos o que añada más métricas/outputs, dime qué necesitas y lo implemento.

- python test.py --duration 60 --batch-size 50