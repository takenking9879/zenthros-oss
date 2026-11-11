# Pipeline de ejemplo: de tres tablas a tres modelos, con Polars y con Spark

Un pipeline de datos completo y pequeño para correr en Zenthros: lee tres tablas, las une con SQL, calcula features, entrena tres modelos y deja los resultados en una carpeta. Está escrito dos veces, una con **Polars** y otra con **Spark**, y las dos versiones producen la misma tabla.

Sirve para dos cosas: como plantilla de un pipeline que no se queda sin memoria, y como carga real para probar un backend (tu computadora en un contenedor, o una máquina Spot).

## En 30 segundos

- **Qué hace:** clientes, pedidos y líneas de pedido → gasto por cliente y mes (SQL con dos `INNER JOIN`) → 21 features → regresión lineal, random forest y gradient boosting, que predicen el gasto del mes siguiente.
- **Dos tamaños, por configuración:** `small` (2 CPU, 4 GB; 4,5 millones de líneas) y `full` (4 CPU, 8 GB; 24 millones). Nada más en el código lleva un tamaño.
- **Es perezoso de verdad:** de la lectura a la escritura ninguna tabla pasa por Python. El único sitio donde una tabla se carga en memoria es el entrenamiento, y ahí hay un tope de filas.
- **No usa más de lo que tiene:** hilos y memoria salen del menor entre el perfil y el límite real del contenedor. Nunca de `os.cpu_count()`.
- **Los resultados son Parquet,** en una carpeta por paso con un marcador de fin.
- **Probado en AWS real** (14 de octubre de 2026): `small` en un contenedor de este equipo con S3 de verdad, en 60 s con Polars y 82 s con Spark; `full` en una Spot c6i.xlarge, en 178 s y 198 s.
- **Desde un notebook:** `flow.ipynb` entra con tu clave, mira dónde puedes correr (`zenthros.targets()`), sube los datos y corre dos funciones decoradas aquí, en un contenedor de tu computadora y en una Spot creada para la llamada.

## Archivos

| Archivo | Qué es |
|---|---|
| `profiles.py` | Los dos tamaños: cuántos datos y cuánto de la máquina puede usar cada motor. |
| `limits.py` | Lee el límite real de CPU y memoria del contenedor (cgroup). |
| `store.py` | Dónde viven las tablas: una carpeta o `s3://`. Un solo sitio sabe cómo apuntar cada biblioteca. |
| `gen_data.py` | Genera las tres tablas de origen, deterministas y un archivo cada vez. |
| `queries.py` | Lo que se calcula, dicho una vez para los dos motores: el SQL y el esquema del resultado. |
| `pipeline_polars.py` | El pipeline con Polars. |
| `pipeline_spark.py` | El pipeline con PySpark en modo local. |
| `spark_jars.py` | Descarga y verifica el conector de S3 de Spark. |
| `train.py` | Los tres modelos, sobre la tabla de features de cualquiera de los dos motores. |
| `pipeline.py` | Los dos pasos que el notebook manda a una máquina, uno por motor: `models` (Polars y los tres modelos) y `tables` (Spark). Cada uno una vez, con su marcador de fin. |
| `requirements.txt` | Todas las bibliotecas fijadas: las del notebook y las de los tests. |
| `requirements-polars.txt`, `requirements-spark.txt` | Lo que necesita cada función del notebook, con las mismas versiones. |
| `Dockerfile` | La imagen donde corren los tests de Spark (Python 3.13, Java y las bibliotecas fijadas). |
| `flow.ipynb` | El recorrido desde un notebook: dónde puedes correr, tus datos, dos funciones aquí, en tu computadora y en una Spot, y borrar. |

## Los datos

Tres tablas sintéticas en Parquet, bajo `<base>/raw/`:

| Tabla | Columnas |
|---|---|
| `customers` | `customer_id`, `segment`, `country`, `signup_date` |
| `orders` | `order_id`, `customer_id`, `order_date`, `channel`, `status` |
| `order_lines` | `order_id`, `line_no`, `category`, `quantity`, `unit_price`, `discount` |

- **Deterministas:** el archivo *i* de una tabla depende solo del perfil, de la semilla y de *i*. Dos máquinas generan los mismos bytes, así que los dos motores leen lo mismo.
- **Un archivo cada vez:** generar nunca tiene en memoria más que un archivo.
- **Hay algo que aprender:** cada cliente tiene tres rasgos fijos que salen de su identificador. Son cuánto le gusta comprar, cuánto paga por lo mismo y una categoría favorita. Además el año tiene estación. Por eso lo que un cliente hizo dice algo de lo que hará.
- **Un manifiesto al final** (`_manifest.json`): tablas sin manifiesto son una generación que no terminó, y se rehacen.

## Lo que calcula

**1. SQL** (`queries.MONTHLY_SQL`, el mismo texto en los dos motores): pedidos completados, unidos a su cliente y a sus líneas, agregados por cliente y mes. Cuenta pedidos distintos y líneas, y suma unidades, gasto y gasto por categoría.

**2. Features** (una fila por cliente y mes en que compró):

| Grupo | Columnas |
|---|---|
| El mes | `n_orders`, `n_lines`, `units`, `spend`, `avg_order_value`, `avg_line_value` |
| Reparto del gasto | `share_electronics`, `share_grocery`, `share_fashion` |
| El mes anterior | `prev_spend`, `prev_orders` |
| Los tres últimos meses | `spend_3m`, `orders_3m` |
| Todo hasta hoy | `spend_cum`, `orders_cum` |
| Tiempo | `months_since_prev` (−1 si es su primera compra), `tenure_months` |
| Estación | `month_sin`, `month_cos` (el mes como punto de un círculo: diciembre queda junto a enero) |
| Categóricas | `segment`, `country` |
| Objetivo | `target_next_spend`: lo que gasta el mes siguiente; cero si no compra |

- **Los meses son los del calendario, no las filas.** Un cliente que se saltó un mes no tiene "mes anterior", diga lo que diga su fila anterior. Los tests lo comprueban contra el mismo cálculo hecho con pandas.
- **El último mes se descarta:** no tiene mes siguiente, así que no tiene objetivo.

**3. Resumen:** clientes, pedidos y gasto por segmento y país.

**4. Modelos** (`train.py`): regresión lineal, random forest (el que tarda y usa las CPU) y gradient boosting por histogramas. Se puntúan sobre los dos últimos meses, que no vieron. El resultado trae dos referencias para leer las puntuaciones: predecir siempre la media, y predecir el gasto del mes anterior.

## Por qué no se queda sin memoria

Dentro de un contenedor limitado a 2 CPU, `os.cpu_count()` dice las del equipo entero (24 aquí). Un pipeline que se dimensionó con ese número lanzó 24 procesos en 4 GB y lo mató el sistema. Este pipeline se protege en cuatro sitios:

- **Hilos:** `limits.cpus()` devuelve el menor entre lo que pide el perfil y la cuota más baja entre el cgroup del proceso y los de encima. Hace falta subir: en una máquina que es a su vez un contenedor (las de Floci), el job corre en una unidad de systemd y el límite está en el cgroup de la máquina. Leyendo solo la raíz, una m5.large de 2 CPU parecía de 4, y el entrenamiento corrió con el doble de hilos. Lo usan Polars, Spark, el random forest y las bibliotecas numéricas.
- **Polars:** el plan se ejecuta en el motor de streaming, que lee por trozos y escribe según avanza. Polars fija sus hilos al importarse, así que el módulo lo importa después de poner `POLARS_MAX_THREADS`. Polars no tiene un tope de memoria propio que configurar: su techo es el del contenedor.
- **Spark:** memoria de la JVM (nunca más de la mitad de la máquina), hilos de tareas, particiones de mezcla y tamaño máximo de resultado, todo desde el perfil. Spark corre en un proceso aparte que termina con su JVM: parar la sesión no la apaga, y se quedaría con su memoria mientras entrenan los modelos.
- **Entrenamiento:** es el único sitio donde una tabla pasa a memoria, con el tope de filas del perfil. El tope es un filtro que el lector de Parquet aplica al leer (clientes enteros, elegidos por su identificador), no un recorte de una tabla ya leída.

Los conteos del resultado salen de los pies de los archivos Parquet: no se vuelve a calcular nada para contar.

## Los resultados

```
<datos>/raw/...             las tablas de origen (gen_data, y ws.upload si van al almacén)
<salida>/features           la tabla de features
<salida>/summary            el resumen por segmento y país
<salida>/result.json
<salida>/_SUCCESS           se escribe el último
```

- **Sin `_SUCCESS` el paso no terminó:** se empieza de nuevo con la carpeta vacía.
- **Un paso terminado no se repite:** una llamada enviada dos veces, o reintentada en otra máquina porque le quitaron la Spot, devuelve lo que dejó el primer intento.
- **Por qué Parquet y no Iceberg:** para que Polars y Spark compartan una tabla Iceberg hace falta un servicio de catálogo aparte, y sus jars pesan cientos de MB más. Con una carpeta por paso y un marcador de fin no hace falta.

## Medido

### En este equipo, sobre disco local

Cada fila es **una sola corrida** del 13 de octubre de 2026, en un contenedor de esta imagen con el límite duro indicado (`--cpus`, y `--memory` igual a `--memory-swap`).

| Perfil | Motor | Límite | Total | Generar | Features | Entrenar | Pico de memoria | Muertes por memoria |
|---|---|---|---|---|---|---|---|---|
| `small` | Polars | 2 CPU, 4 GB | 25 s | 2,8 s | 2,8 s | 19,7 s | 1,2 GB | 0 |
| `small` | Spark | 2 CPU, 4 GB | 52 s | 2,7 s | 30,0 s | 19,4 s | 2,4 GB | 0 |
| `full` | Polars | 4 CPU, 8 GB | 153 s | 14,2 s | 12,3 s | 126,2 s | 4,1 GB | 0 |
| `full` | Spark | 4 CPU, 8 GB | 187 s | 14,2 s | 49,5 s | 123,4 s | 4,9 GB | 0 |

- **El pico de memoria** es el del cgroup del contenedor (`memory.peak`), que cuenta también la caché de archivos: es una lectura por arriba.
- **Filas de features:** 496.003 en `small` y 3.461.216 en `full`, las mismas con los dos motores.
- **Casi todo el tiempo de entrenar es el random forest:** 18,7 s de 19,7 s en `small` y 121,7 s de 126,2 s en `full`.
- **Los modelos aprenden lo mismo con los dos motores:** R² de 0,45 a 0,47 en `small` y de 0,39 a 0,44 en `full`, contra 0 de predecir la media.
- **La imagen pesa 3,65 GB.** De eso, 1,2 GB es PySpark, y 654 MB de ese 1,2 GB es el SDK de AWS que exige el conector de S3.

### En AWS, con las tablas en S3

El E2E del flujo anterior (un contenedor unido con `LocalBackend`, credenciales selladas con `zctl`) del 14 de octubre de 2026, en us-east-2. El pipeline es el mismo. Hay dos corridas: la segunda falló en Spark sobre la Spot (abajo) y la tercera pasó entera. Los tiempos son los del job, en segundos; *generar* casi a cero significa que las tablas de origen ya estaban (las escribió el otro motor).

| Dónde | Perfil | Motor | Total | Generar | Features | Entrenar |
|---|---|---|---|---|---|---|
| Contenedor de este equipo, 2 CPU y 4 GB | `small` | Polars | 60 s y 74 s | 27 s y 28 s | 9 s y 20 s | 24 s y 26 s |
| Contenedor de este equipo, 2 CPU y 4 GB | `small` | Spark | 82 s y 85 s | 0,4 s | 56 s y 60 s | 26 s y 25 s |
| Spot c6i.xlarge, 4 CPU y 8 GB | `full` | Polars | 178 s y 181 s | 24 s | 18 s | 136 s y 139 s |
| Spot c6i.xlarge, 4 CPU y 8 GB | `full` | Spark | 198 s (una corrida) | 0,1 s | 63 s | 135 s |

- **El contenedor escribe en S3 desde este equipo,** a través de Internet: por eso *generar* tarda 27 s frente a 2,8 s en disco local.
- **La Spot vio 4 CPU y 7,6 GiB,** y el pipeline usó eso, no el perfil a ciegas.
- **El disco de una máquina de AWS es de 8 GB.** Los paquetes de este pipeline ocupan 1,07 GB instalados, y el instalador dejaba además 2,46 GB de caché de `uv` (medido en un contenedor). Con eso, Java y el conector de S3 (654 MB), la etapa de Spark murió en la segunda corrida sin poder escribir ni su error (código 120). El instalador ya no guarda la caché, y si la etapa vuelve a morir sin decir nada, el error dice cuánto disco quedaba libre.
- **Las credenciales de S3 cubren un prefijo, no el bucket.** Por eso el pipeline no crea "carpetas" en S3: pyarrow lo hace preguntando por el bucket entero (`HeadBucket`), y AWS lo niega. Floci no aplica permisos, y allí no se vio.

## Probarlo

En tu equipo, sin clúster ni nube:

```bash
cd sdk/python/examples/pipeline
python3 -m unittest discover -s tests -t tests     # Polars, datos, límites y entrenamiento
python3 gen_data.py /tmp/pipeline small            # solo generar los datos
```

Spark necesita Java, así que sus tests corren dentro de la imagen, con los mismos límites que tendrá el backend:

```bash
docker build -t zenthros-pipeline:local examples/pipeline
tar -c --exclude=__pycache__ examples/pipeline zenthros pyproject.toml |
  docker run --rm -i --init --cpus 2 --memory 4g --memory-swap 4g zenthros-pipeline:local \
  sh -c 'mkdir -p /tmp/src && cd /tmp/src && tar -x && python -m unittest discover -s examples/pipeline/tests -t examples/pipeline/tests'
```

La imagen corre sin root: el código entra por la entrada estándar y se abre en `/tmp/src`.

`--memory` y `--memory-swap` iguales hacen que el límite sea duro: si el pipeline se pasa, muere él dentro del contenedor y no el equipo. `--init` pone un proceso 1 que recoge a los huérfanos, como en el backend: sin él, un test que deja un `java` huérfano lo ve como una JVM que no terminó.

## El flujo desde un notebook

`flow.ipynb`, en esta carpeta, es código normal. Es el mismo recorrido que el E2E (`tests/e2e/test_notebook_flow.py`), que ejecuta la celda de funciones del cuaderno tal cual y comprueba cada paso:

1. `zenthros.login()` (la primera vez, `zenthros.login(key="zen_…")`) y `zenthros.targets()`: dónde puedes correr, con lo que tiene una máquina de cada destino y cuántas puedes usar o crear.
2. `gen_data` genera los datos aquí y `ws.upload` los sube al almacén del workspace.
3. Dos funciones decoradas con lo que necesitan: `entrenar` (`requirements="requirements-polars.txt"`) y `tablas_spark` (`requirements="requirements-spark.txt"`, `tools=["java"]`).
4. `entrenar(...)` aquí; `zenthros.create("local", name="mi-pc", cpus=4, memory="8Gi")` y las dos en ese contenedor; `on="aws-spot"` con `dry_run=True` primero, y después en una máquina que el pool crea para la llamada y apaga al quedar libre; una VM que se queda (`create("aws-spot", name="vm-1")`); tu propia VM con `zenthros.attach`; y `zenthros.delete`.

- **Dónde:** el mismo cuaderno en Floci (después de `up-local.sh` y `up-floci.sh`, que crea la plantilla `aws-spot`) y en AWS. Nada en él dice dónde está S3: el hub da a cada contenedor una hora de credenciales de su almacén, selladas para él, y su región y endpoint; la máquina de un pool usa su rol.
- **Permisos:** tu administrador te da una clave (`zctl user add yo --workspace W --role editor --machines --provision`): `--machines` para unir contenedores y VMs tuyas, `--provision` para crear máquinas desde plantillas.
- **Un clúster de Kubernetes del workspace:** `targets()` lo muestra por nodo, nunca la suma, porque un job corre en un solo nodo. CPU y RAM son las de su nodo más grande; AVAILABLE cuenta los nodos con al menos 2 CPU y 4 GiB libres, de cuántos tiene; el detalle (`zenthros.targets()["cluster-b"]`) lista cada nodo con lo que le queda. `dry_run=True` cuenta en cuántos nodos cabe lo que pide la función. `zenthros.create("k8s:cluster-b", name="mi-pod", cpus=1, memory="2Gi")` es un pod de ese clúster como máquina tuya: un Job con esos límites (no solo pedidos) que instala el agente y se une con un token que el hub sella para el agente del clúster; el token nunca está en el job. `zenthros.delete` lo da de baja y después borra el Job y su pod. Lo comprueba `tests/e2e/test_k8s_pod_flow.py` en el k3s de `up-local.sh`.
- **Los datos locales, fuera de esta carpeta:** todo lo que hay en ella viaja con cada llamada (`code_dir`).

## Cómo llegan las bibliotecas a la máquina

La llamada lleva la función y los `.py` de esta carpeta. Las bibliotecas las instala la máquina, una vez por función:

- **El entorno de una función** es un venv con las líneas de su `requirements=` (o `packages=`), guardado por su hash en la máquina y reutilizado en la siguiente llamada. Hereda lo que el runner necesita (cloudpickle, boto3) del Python de la máquina. `tools=["java"]` pone el Temurin que fija el instalador (Spark 4.2 pasó sus tests con él).
- **Contenedor de tu computadora:** `create("local")` construye una vez `zenthros-python:<3.13>-<hash>`, Python del mismo menor que el notebook (una función serializada allí se abre aquí) con lo que el runner necesita.
- **Máquina de una plantilla:** los `toolchains` de la plantilla: `python` 3.13 con boto3 y cloudpickle fijados. La de Floci la escribe `up-floci.sh`.
- **El conector de S3 de Spark** no viene en el `pyspark` de PyPI. `spark_jars.py` descarga `hadoop-aws` y lo que su POM pide para compilar, en las versiones que fija Hadoop (el SDK de AWS y, desde Hadoop 3.4.2, el acelerador de lectura de S3; sin este, la primera lectura de `s3a://` falla con `NoClassDefFoundError`), los comprueba contra el SHA-1 que publica Maven Central y los deja donde Spark los encuentra: la primera etapa de Spark de una máquina los descarga (unos 660 MB) a `/tmp/zenthros-pipeline-jars` (o a `PIPELINE_JARS_DIR`), que se conserva entre llamadas, y los pasa a Spark con `spark.driver.extraClassPath`.
