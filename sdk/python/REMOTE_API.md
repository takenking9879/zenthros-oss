# La API remota de Zenthros

**Idea central:** una sola función, `zenthros.run`. Acepta tres tipos de trabajo:

- una función;
- una `Task`;
- un comando.

Las opciones son de un solo vocabulario para los tres. El resto son objetos que ya existían: `RemoteCall`, `Task` y `Checkpointer`.

Todo está implementado en `zenthros/remote/`:

| Archivo | Qué tiene |
|---|---|
| `remote/call.py` | `run`, `map`, `gather`, `get`, `cloud`, `RemoteCall`, `Cost` |
| `remote/spread.py` | `f.map(configs, on=[...])` repartido entre destinos (`MapCalls`), `grid` |
| `remote/runlog.py` | el registro local de cada llamada (`.zenthros/runs/`), `last_failure` |
| `remote/servicelist.py` | `zenthros.services()`: lo que está sirviendo y cuánto lleva costado |
| `remote/options.py` | `configure`, `Secret` |
| `remote/bundle.py` | lo que viaja con la llamada |
| `remote/runner.py` | lo que corre en la máquina |
| `remote/envs.py` | el entorno de cada función (`requirements=`, `packages=`, `tools=`) |
| `auth.py`, `view.py` | `login`, `workspace()`: `upload`, `ls`, `download`, `jobs` |
| `signin.py` | `login(server=…)` sin key: login con Google en un hub con sign-ups, por una página en localhost |
| `machines.py`, `targetlist.py` | `create`, `delete`, `attach`, `targets` y los `dry_run` |
| `files.py` | `zenthros.files`: leer y escribir el store por URI, aquí y en un job |
| `seal.py` | sellar con age en Python, para `Secret.encrypted` |

Los tests sin cluster están en `tests/` (`test_remote*.py`, `test_on.py`, `test_targets.py`, `test_machines.py`, `test_files.py`, `test_map_configs.py`, `test_cost.py`, `test_seal.py`, `test_serve.py`, `test_services.py`, `test_runlog.py`, `test_report_progress.py`, `test_interrupt.py`, `test_local.py`). Los E2E, en `tests/e2e/` (`test_notebook_flow.py`, `test_serve_flow.py`, `test_remote_demo_notebook.py`, `test_k8s_pod_flow.py`) y `examples/remote/suite.py`.

**Lo que el SDK no puede hacer solo lo hace zenthros-api** (`operator/docs/api.md`): `targets`, las máquinas de plantilla, el coste de un job, y las URLs firmadas del store (también para listar). Del controller, el SDK solo pide una cosa propia: parar un Service a su hora (`stop_after`, decisión 24).

---

## 1. Qué hay, y a qué campo del CRD va

| Capacidad | En el SDK | En el CRD |
|---|---|---|
| Recursos | `cpu`, `memory`, `gpu="A10G"\|n`, `instance_type`, `spot` | `spec.compute` |
| Runtime | `image=` (el controlador elige el runtime: containerd en un pool VM, el propio contenedor en un pool Container), `python=`, `code_dir`; guarda de versión de Python; aviso si los paquetes difieren | `template.image`, `compute.runtime` |
| Env | `env={}` | `template.env` |
| Secretos | `secrets={"DB_URL": Secret.aws(...)}`, o `{"/run/secrets/x.json": Secret.k8s(...)}` para un archivo | `template.secretSources` |
| Máquina perdida / Spot | `resilient=True`, el default | `resilience.onWorkerLost=Reschedule`, con el límite del controller |
| Reintentos de tu código | `retries=3, retry_on=ConnectionError`, en el runner | nada (en funciones) / `restartPolicy.backoffLimit` (en comandos) |
| Checkpoints | `Task`, `Checkpointer`, `@zenthros.phase`, `stop_on_notice` | cursores, COMPLETE, aviso |
| Timeout | `timeout="2h"` | `activeDeadlineSeconds` |
| Cuánto dura un job terminado | `keep_for="7d"` (máximo 90 d); sin él, el default del controller: 1 h | `gcPolicy.ttlSecondsAfterFinished` |
| Dependencias | `after=[call]`, o un `RemoteCall` como argumento | `dependsOn` |
| Paralelo | `zenthros.map(fn, items)` y `gather` | `distribution.count`, `TASK_INDEX` |
| Ciclo de vida | `result`, `status`, `logs`, `reports`, `info`, `cancel`, `delete`; `zenthros.get(name)` | ✅ |
| Command API | `zenthros.run("python train.py")` o `run(["python", "train.py"])` | `template.command` |
| Servicios | `zenthros.serve(target, port=8000, on=..., stop_after="4h")`, `call.url()`, `call.endpoints()`, `with ... :` cancela; `zenthros.services()` | `template.service.ports`, `template.service.stopAfterSeconds`, `status.endpoints`, `ServiceReady` |
| Secreto sellado (RunPod, Vast.ai, contenedor local) | `Secret.encrypted(valor, on=máquina)`: con `zenthros[seal]` (cryptography) se sella aquí, en Python, a las claves age que la API lista para esa máquina; sin él, con zctl. `Secret.encrypted(valor, pool="gpu")` y `recipients=[...]` también. `zenthros.aws.s3_secrets("s3://bucket/prefijo/", zenthros.workspace().store, write=True, backend=id)`: una hora de STS limitada a esos prefijos, sellada | `secretSources[].encryptedPayload` |
| Secreto del Workspace (una plantilla, una máquina que despierta) | `ws.secrets["hf-token"] = valor` una vez (un editor); `secrets={"HF_TOKEN": ws.secrets["hf-token"]}` o `zenthros.Secret.workspace("hf-token")`; `del ws.secrets["hf-token"]`. El valor no vuelve nunca | `secretSources[]` `type: workspace-secret`, `name`: el controller lo sella a la máquina al mandarle el job |
| Dónde corre | `on=` en `run`, `serve`, `submit`/`remote` y `map`: una máquina (`create`), el nombre de una, de un pool o de una plantilla; `on=[...]` en `map` reparte entre varios. Sin `on=`: `configure(on=)` o `ZENTHROS_ON`, si no esta computadora (`"local"`). Es la única forma: `pool=`, `backend=` y `acquire=` se quitaron y dan un error que dice `on=`. `image=` en una máquina que no corre imágenes se rechaza antes de mandar nada, y un nombre que es máquina y plantilla a la vez también. Antes de crear nada, una plantilla con otro Python menor que el de aquí se rechaza | `capacity.poolRef` / `backendID`, `capacity.acquire` |
| Base de la máquina | `zenthros.install(nombre, requirements=, packages=)`: un venv por menor de Python en la máquina (`envs.install_base`) | ninguno: corre como una función más |
| Entorno de la función | `requirements="requirements.txt"`, `packages=[...]`, `tools=["java"]`: un venv por función, en caché en la máquina; un paquete sin versión se fija a la de esta computadora (`call._pinned`), salvo si no está instalado aquí o vino de una carpeta o una URL | `template.env` (el runner lo prepara) |
| Argumentos con nombre | `params={"datos": uri}` en `submit`/`remote`; `shared=` en `map` | la llamada (`call.pkl`) |
| Coste | `call.cost()`: lo que cuenta zenthros-api | `status.lease`, la CapacityRequest y el catálogo |
| Progreso | una línea por report nuevo mientras `result()` espera; columna `report` en la tabla de un `MapCalls`; `calls.reports()`, una fila por config | `zenthros.report` (ExecutionReport) |
| Lo que pasó, en disco | `.zenthros/runs/<job>/`: `call.json`, `logs.txt`, `reports.jsonl`, `error.txt`; `zenthros.last_failure()` | nada: lo escribe el SDK aquí |

---

## 2. La superficie

**Sin cuenta: este computador.** Sin `on=` (o con `on="local"`), una llamada corre aquí, con sesión iniciada o sin ella. Solo hace falta Docker o Podman (en Windows, dentro de WSL2):

```python
import zenthros

@zenthros.cloud
def suma(a, b):
    return a + b

print(suma.remote(2, 3))          # 5; .submit() da el handle: call.result(), .status(), .logs(), .cancel()
```

- **Corre** con el mismo agente que en una máquina remota, sin hub: un contenedor por proyecto e imagen, creado la primera vez, y el agente verificado contra el pin de la wheel (`install/build-sdk.sh`). Después no necesita red. Detalle en `operator/docs/design/local-mode.md`.
- **Tamaño:** el contenedor tiene las CPUs del motor y 3/4 de su memoria, y es el tope de todo lo que corra adentro. `configure(local_cpus=, local_memory=)` lo cambia, y otro tamaño es otro contenedor. Un `memory=` mayor que el contenedor se rechaza antes de correr.
- **Archivos:** una ruta pasada como argumento se monta en solo lectura en la misma ruta; nada más de este computador entra, y `.env` y las llaves se rechazan. Registros y resultados, en `.zenthros/local`. `$ZENTHROS_DATA_OUTPUT` es una carpeta de ahí, o de `configure(local_output=)`.
- **Secretos:** `Secret.encrypted(from_env="API_KEY", on="local")` (con `zenthros[seal]`) queda en memoria y se sella al arrancar el job con la clave del agente. Ningún archivo guarda el valor, y no sirve para otro destino.
- **Se comporta igual que en remoto:** `retries=`, `timeout=`, `stop_after=`, `serve` y sus razones de fin; `OOM_KILLED` dice que subas `configure(local_memory=)`. Un job sin red que necesita algo que falta dice "run this once with network".
- **Retención:** un job dura `keep_for` (7 días por defecto); lo borra el SDK en su siguiente comando, no un demonio. Sus archivos quedan hasta que los pidas borrar.
- **`zenthros clean`** borra los contenedores del proyecto, salvo los que tienen jobs corriendo; `--results` lo que queda en el store sin registro, `--envs` los Python sin contenedor. `--dry-run` y `--json` dicen qué haría.
- **Se detiene** si apagas el computador. Para un job que siga sin él, una máquina remota.

**Sin cuenta: el Kubernetes que ya tienes.** `on="local-k8s"` usa el contexto actual de `kubectl` (`configure(kube_context=)` lo cambia), y `targets()` lo muestra. Cada job es un pod con su propia imagen, y las llamadas se encadenan igual que en un hub.

- **Instala** el mismo chart de `attach --type kubernetes`, con `agent.local`, en el namespace `zenthros-local`, la primera vez y tras preguntar (`ZENTHROS_LOCAL_K8S_INSTALL=1` donde no hay terminal). El chart se verifica contra el pin de la wheel, que también nombra la imagen del agente. El agente no tiene permisos fuera de su namespace, salvo leer los nodos.
- **Habla** con él por `kubectl exec`: quien puede hacer eso ya manda en ese namespace.
- **Archivos:** `~/.zenthros/k8s` tiene que estar en cada nodo en la misma ruta (el `-v` de `k3d cluster create` del README). El SDK lo comprueba con una marca; sin esa carpeta, el error da el comando. Una ruta fuera de ella se rechaza antes de crear nada. Lo que el job escribe queda con tu usuario. `configure(local_output=)` no aplica aquí.
- **Recursos:** `memory=` es request y límite; `cpu=`, request. Sin lugar para un pod: `Unschedulable` y "Insufficient memory". Una GPU que ningún nodo ofrece o una imagen de otra arquitectura se rechazan antes (la arquitectura solo si el registry responde).
- **Reintentos:** los lleva Kubernetes (`backoffLimit`).
- **Todavía no:** `serve` (usa `local`).
- **`zenthros clean --k8s`** quita el agente y el namespace, salvo con jobs corriendo.

**Entornos: tus paquetes en una imagen, repetible y publicable.** `zenthros.env` y `zenthros env`: `new`, `add`, `build`, `use`, `push`, `list`. No hay entidad en el hub: todo vive en `envs/<nombre>/env.lock.json`, en Git.

- **Dos formas, un solo build:** sin `Dockerfile` en `envs/<nombre>/`, Zenthros lo escribe; con él, se usa ese. Mismo builder (`zenthros`, BuildKit en un contenedor, con 20 GB de tope; en Podman, `podman build`) y misma caché. Lo que parece una credencial en esa carpeta no entra al build.
- **Versiones:** `env build` pregunta a uv sobre la base misma, solo cuando cambian los paquetes pedidos, y guarda el resultado en el lock (`pins`). Lo que la base ya trae entra como restricción: no se baja de nuevo y un paquete no puede reemplazarlo. Un paquete nuevo no mueve los demás; `env add --upgrade` sí.
- **Sin cambios no se llama al builder,** así que funciona sin red. Sin red, un paquete nuevo lo dice y la imagen anterior sigue corriendo, con un aviso.
- **Dos digests:** el de contenido (la config OCI, lo que corre) es la identidad del entorno y no cambia al recomprimir; el de manifiesto (lo que se baja) sí. `env push` lee lo publicado y compara: `identical`, `same content`, `same files, only metadata differs`, o `same versions, not the same bytes` con `--platform`. Si es otra cosa, lo rechaza. También rechaza un entorno cambiado sin construir, y una caché perdida (antes de publicar nada: `env build --force`, probar, publicar).
- **Sin la imagen en esta computadora,** `env push` copia lo que el lock dice publicado, sin build.
- **`env="train"`** en `run`, `@cloud` o `--env`. En `local`, la imagen construida. En `local-k8s`, esa imagen publicada en el registry del cluster, el que el cluster declara en `local-registry-hosting` (`k3d --registry-create` lo escribe). En otro destino, lo último publicado, por digest. `image=` gana. Una función necesita el mismo Python menor que quien llama; un comando no.
- **`env use`** deja la imagen, el agente y el Python del runner listos para trabajar sin red, y las llamadas locales sin `image=` corren ahí. `env use --off` lo deshace. Es de esta computadora: no va al lock.
- **`zenthros clean --images`** borra las imágenes de este proyecto y el builder con su caché, que comparten todos los proyectos de esta computadora. La que un contenedor usa se queda.

**Con cuenta.** Un usuario con una clave de su admin (`zctl user add … --machines --provision`) no tiene más que esto:

```python
import zenthros
from zenthros import files

ws = zenthros.login()                       # la primera vez: zenthros.login(key="zen_…"); queda en su zenconfig
zenthros.set_store("s3://mi-bucket/zenthros")   # un hub sin store (registro con Google): su bucket, una vez; queda en su zenconfig
zenthros.targets()                          # dónde puede correr: local, sus máquinas, pools, plantillas, clusters

datos = ws.upload("./datos", to="demo/datos")                 # URLs firmadas: sin claves de nube aquí
mi_pc = zenthros.create("local", name="mi-pc", cpus=4, memory="8Gi", ports=[8000])   # un contenedor aquí; 8000 en 127.0.0.1
vm = zenthros.create("aws-spot", name="vm-1", idle="30m")              # una VM de plantilla que duerme sin jobs
zenthros.create("aws-spot", dry_run=True)   # qué haría, su tamaño y su precio; no crea nada

@zenthros.cloud(requirements="requirements.txt")
def entrenar(datos, salida, ventanas=(5, 20)):
    df = pd.read_parquet(files.open(datos))           # igual aquí (ruta local) que en la máquina (s3://)
    files.write_bytes(f"{salida}/modelo.pkl", pickle.dumps(modelo))
    return metricas

entrenar("./datos/x.parquet", "./salidas")                                   # aquí, como cualquier función
call = entrenar.submit(on=mi_pc, params={"datos": datos, "salida": f"{ws.store}/salidas"})
call.result()   call.cost()   call.overhead()
entrenar.remote(on="aws-spot", params={...})        # una máquina de la plantilla para esta llamada; se apaga libre
entrenar.remote(on="aws-spot", params={...}, dry_run=True)

configs = zenthros.grid(ventanas=[(5, 20), (20, 60)], profundidad=[4, 8])   # 4 configs: el producto
calls = entrenar.map(configs, on=["aws-spot", "mi-pc"], shared={"datos": datos, "salida": ...})
calls.result()  calls.cost()  calls   # resultados en el orden de configs; tabla de dónde corrió cada una y su último report
calls.reports()                        # un DataFrame: una fila por config, con su último report
zenthros.get(calls.name)               # el mismo MapCalls, desde otra sesión

zenthros.run("python train.py --epocas 10", on="aws-spot")   # un script que ya tienes
api = servir.serve(on=mi_pc, port=8000, stop_after="4h")      # un Service: el controller lo para a las 4 h
api.url(health="/health")                                     # http://127.0.0.1:8000
zenthros.services()                                           # lo que está sirviendo, cuánto lleva y su coste
zenthros.last_failure()                                       # la carpeta de la última llamada que falló

ws.ls("salidas/")                          # [StoredObject(uri, size, modified)]
ws.download(f"{ws.store}/salidas", to="./salidas")   # una carpeta, o los bytes de un objeto
zenthros.delete(vm)   zenthros.delete(mi_pc)
zenthros.attach("mi-ec2", type="vm", runtime="containerd", host="ubuntu@10.0.0.5")   # una máquina que ya tienes: por ssh, o las dos líneas sin host
zenthros.install("mi-ec2", requirements="requirements.txt")   # lo que toda función ve en esa máquina; «up to date» si no cambia
# en la terminal: zenthros attach mi-ec2 (pregunta lo que falte), zenthros targets, zenthros detach mi-ec2, zenthros install mi-ec2 -r requirements.txt
```

Lo mismo, por debajo, con todas las opciones:

```python
zenthros.configure(store="s3://acme-zenthros/calls", workspace="risk")   # la vía de admin, o variables de entorno

call = zenthros.run(
    target,                     # una función, una Task, o un comando: "python x.py" / ["python", "x.py"]
    *args,                      # los argumentos (solo con función o Task)
    on="gpu-box",               # dónde: una máquina o plantilla; sin on=, la única máquina del Workspace
    cpu=8, memory="32Gi", gpu="A10G", spot=True,
    image="ghcr.io/acme/train:1.4",          # opcional; solo en una máquina que corre imágenes (containerd, un clúster)
    env={"LOG_LEVEL": "info"},
    secrets={"DB_URL": zenthros.Secret.aws("prod/db", key="url")},
    retries=2, retry_on=(ConnectionError,),
    timeout="2h",
    after=[prep],
    name="train-v3", labels={"team": "risk"},
)

call.result(timeout=None)   call.status()   call.logs(tail=None)   call.reports()   # Ctrl-C en result() cancela el job
call.info()                 call.cancel()   call.delete() call.output
zenthros.get("train-v3")                                  # la misma llamada, desde otra sesión (mientras el job exista)
zenthros.map(fn, items, **opciones_de_run) -> RemoteCall  # result() es la lista, en orden
f.map(items, on=None | destino | [destinos], shared={}, dry_run=False)   # un RemoteCall, o MapCalls con varios
zenthros.gather(*calls, return_exceptions=False) -> list

@zenthros.cloud(gpu="A10G")                               # el mismo vocabulario, como decorador; on= va en cada llamada
def f(...): ...

llm = zenthros.serve("vllm serve m --port 8000", on="gpu-box", port=8000,   # un Service: corre hasta cancel
                     secrets={"VLLM_API_KEY": zenthros.Secret.encrypted(key, on="gpu-box")})
llm.url(timeout=600)        # espera ServiceReady y devuelve la URL; se relee en cada llamada
llm.url(health="/health")   # además espera un 2xx de la app en esa ruta, a través del proxy del provider
llm.endpoints()   llm.logs()   llm.cancel()      # with llm: ... cancela al salir
```

`serve` acepta lo mismo que `run`, menos `timeout` y `retry_on`:

- `port=8000` publica un puerto llamado `http`; `ports={"api": 8000, "m": (9090, "TCP")}` o `[8000, 9090]` publican varios.
- `retries=2` (el default) vuelve a correr el proceso si termina, aunque salga con 0.
- Una función que sirve (`uvicorn.run(app)`) también vale.
- `result()` de un Service es `TypeError`: nunca termina. Tampoco puede ser `after=` ni argumento de otra llamada; pásale su `url()`.
- Cada corrida publica su propia URL. Después de una re-corrida o de un reemplazo de máquina, `url()` devuelve la nueva.
- Publicada significa que el puerto acepta conexiones en la máquina. El frente del provider puede ir detrás: el proxy de RunPod responde 404 de 8 a 17 s más (medido). Con `health=` se espera la respuesta de la propia app.
- Sin `secrets=` se avisa por stderr: la URL puede ser pública.
- `on=` lo coloca como a `submit`. Una máquina creada para él (de una plantilla) se borra con `cancel()`, salvo `keep=True`. Sin `on=` corre en este computador, con `port=` publicado solo en `127.0.0.1`.
- `stop_after="4h"`: el controller lo para (`Cancelled`, motivo `STOP_AFTER`) cuando lleva ese tiempo sirviendo, contado desde su primer arranque, aunque el notebook esté cerrado. `zenthros.services()` lista lo que sigue sirviendo.
- En un contenedor de aquí, el puerto tiene que estar publicado: `create("local", ports=[8000])` lo publica solo en `127.0.0.1` y el agente anuncia `http://127.0.0.1:8000`. `serve(on=)` rechaza, antes de mandar nada, un contenedor creado sin ese puerto. En una plantilla, los puertos son los `exposedPorts` de su pool: `targets()["plantilla"]` los dice ("serves on") y `serve(on="plantilla", port=)` en otro puerto se rechaza antes de crear la máquina.
- Actualizar es reemplazar: `api = api.replace(f_v2.serve(on=otra, port=8000), health="/health")` espera a que la nueva responda y solo entonces cancela la vieja. Si la nueva no llega a responder, se cancela ella y la vieja sigue sirviendo. Mientras tanto corren las dos, y un servicio ocupa su máquina él solo: la nueva va en otra máquina. Una en la misma máquina se rechaza al momento (si no, esperaría a la vieja para siempre: `EXCLUSIVE_QUEUED`).

`Secret.encrypted` sella el valor a las llaves age de la máquina que dice `on=` (con kubeconfig, sin zenthros-api, un nombre es un pool: sus máquinas Ready). Una máquina creada después no lo puede abrir, así que con un pool hay que sellarlo con el pool caliente (`minReady`) y correr en él. Con `cryptography` el valor no sale de este proceso; sin él viaja a zctl por su entorno, nunca por argv.

Para una máquina que todavía no existe (una plantilla, una máquina con `idle` que despierta con otra clave) está `ws.secrets`: el valor se guarda una vez en el hub, como un Secret de Kubernetes en el namespace del Workspace, y el controller lo sella a la máquina en el momento de mandarle el job. El spec del job solo lleva el nombre. Un secreto que no existe hace fallar el job con `INVALID_SECRET_SOURCE`, diciendo cómo ponerlo. Como archivo, ponlo bajo `/var/run/zenthros/secrets/` (`secrets={"/var/run/zenthros/secrets/hf/token": s}`) y léelo como `$ZENTHROS_SECRETS_DIR/hf/token`: funciona en cualquier máquina. Otra ruta solo vale dentro del directorio del job cuando la máquina no tiene contenedor (un contenedor de aquí, por ejemplo).

**`ws.clean(older_than="30d", dry_run=False, outputs=False)`.** Nada en el store caduca solo. `clean` borra, con DELETE firmados, lo que es de la llamada en las carpetas de llamadas (`<job>/` con su `call-<sha16>.pkl`) cuyo último objeto es anterior a esa fecha: su resultado y sus checkpoints, que sin el job ya no se pueden reanudar. El resto de la carpeta son las salidas del job (`$ZENTHROS_DATA_OUTPUT` es esa carpeta): se quedan con la llamada, que es lo que dice que esa carpeta es de una llamada, y el informe dice cuántas y cuánto pesan. `outputs=True` las borra también, y la llamada, en ese momento o en un `clean` posterior. No toca una llamada cuyo job sigue en el Workspace, ni `code/`, ni lo que no es una llamada (lo subido con `upload`). La llamada se borra la última: un `clean` cortado termina en el siguiente. `dry_run=True` dice qué borraría; `calls=["exp-41", "exp-42"]` mira solo esas llamadas.

**`f.map(configs, on=[...], shared=)`.** Cada config es un diccionario de argumentos (como `params=`), y cada una es una tarea: `fn(**shared, **config)`. Un nombre en los dos es un error. Un elemento que no es un diccionario es el único argumento, como siempre. Con una lista de destinos:

- el SDK reparte según el `available` de `targets()` (por turnos entre los que tienen sitio; si ninguno tiene, por turnos entre todos);
- hace un job por destino, con su `distribution.count`; con `name=` se llaman `<name>-<k>`;
- si un destino falla al arrancar, cancela y borra los que ya arrancaron;
- `result()` junta los resultados en el orden de `configs`. Si una falla, dice cuál config y dónde corrió; `result(return_exceptions=True)` devuelve las demás.
- Sus jobs se llaman `<name>-<k>` (sin `name=`, el SDK le da uno: `calls.name`) y llevan la etiqueta `zenthros.ai/map=<name>`; cada uno anota qué configs tomó y dónde (`zenthros.ai/map-indices`, `map-on`). `zenthros.get(name)` lo reconstruye: los configs los lee de la llamada guardada de cada job, que `get` ya comprueba por su digest.
- Un `MapCalls` como argumento de otra llamada es un error (pasa `calls.result()`); `after=[calls]` espera a todos sus jobs.

**`create(..., idle="30m")`.** La máquina de plantilla no se crea hasta su primera llamada y termina tras ese tiempo sin jobs. El nombre queda: `targets()` la muestra `Available` ("asleep") y la siguiente llamada la enciende. Sin `idle`, una máquina de plantilla vive hasta `delete`.

**`call.cost()`.** Tiempo corriendo × el último precio que el catálogo observó para la oferta de la que salió su máquina. Sin precio (una máquina tuya, local, un cluster, o un catálogo sin esa oferta) no da número: da el motivo (`basis`). No cuenta el arranque ni la cola de inactividad de la máquina.

**`zenthros.files` y `ws.ls`/`ws.download`.** Aquí, por URLs que firma la API (también para listar, una página de 1000 por URL); en un job, con el acceso al store que el hub le da al job. Nunca salen del prefijo del Workspace. Una ruta local es un archivo local, para que la misma función corra aquí.

`configure()` se queda con lo que dice **dónde está Zenthros**:

- `store`, `on` (dónde corre una llamada sin `on=`; sin él, la única máquina del Workspace), `workspace`, `namespace`, `python` y `env` (lo que recibe todo job);
- `client`, `blob_store` y `ship_sdk`, para tests.

Cada setting también se puede dar por variable de entorno: `ZENTHROS_REMOTE_STORE`, `ZENTHROS_ON`, `ZENTHROS_WORKSPACE`, `ZENTHROS_REMOTE_ENV`, … Además, `configure()` sirve como context manager: `with zenthros.configure(...):` restaura los valores anteriores al salir.

Dentro de un job no cambia nada: `zenthros.Task`, `zenthros.phase`, `zenthros.report` y `zenthros.spawn`/`children`.

**Un agente de IA: `zenthros mcp`.** Cinco herramientas sobre las mismas funciones (`zenthros/tools.py`), para que un agente mande una prueba a otra máquina y siga trabajando.

```console
$ claude mcp add zenthros -- zenthros mcp
$ zenthros agent allow gcp-lab --max-machines 3     # sin esto, run rechaza lo que cuesta y dice el precio
```

| Herramienta | Qué hace |
|---|---|
| `targets` | Dónde se puede correr, precio, `allowed` y, si no, el comando para permitirlo (`to_allow`) |
| `run` | Vuelve de inmediato con el nombre del job y `code`, el SHA-256 del código que tomó. Corre sobre una foto del worktree, cambios sin commit incluidos: lo que edites después no le llega |
| `status` | `state`, `exit_code` (el del proceso; sin él, falló la máquina y lo dice `reason`), `took_s`, `code`, `cost_usd`, `artifacts` |
| `logs` | Solo las líneas pedidas, con `grep`, `tail` y `limit`. Con la máquina ya apagada, lee lo que el job guardó en el store |
| `cancel` | Solo lo que lanzó esa sesión (`ZENTHROS_AGENT_SESSION`) |

Una sesión de un solo turno, como `claude -p`, no puede esperar: termina con el nombre del job, y `claude -p --resume <sesión>` lo lee después. La foto deja fuera lo que `ignore.py` tiene por credencial y lo lista al mandarla. Un job de agente dura 7 días (`keep_for`).

---

### Caché, `Pipeline` y contratos

| Qué | Cómo |
|---|---|
| Guardar un resultado verificado | `run(..., cache=True)` o `"strict"`; también en `@cloud(...)` y `f.remote(...)` |
| Declarar lo que lee sin recibirlo | `deps=["datos.csv"]`, `deps={"ventas": "2026-10-31"}` |
| Una llamada acertada | `CachedCall`: `.result()`, `.status()` da `"reused"`, `.cached`, `.outs()`, `.to_dict()` |
| Pasos en línea | `zenthros.Pipeline([("a", a), ("b", b)], name=)`: `.run`, `.plan`, `.resume` |
| Por paso | `params={"b": {"depth": 8}}`, `on={"a": "local", "b": "gcp-gpu"}` |
| Reusar a mano | `start_at="b", upstream={"a": ruta | URI | get(...) | cache.find(...)}` |
| Contratos | `inputs=`, `outputs=`: `"parquet"`, `"json"`, `"file"`, `"dir"`, `zenthros.parquet(columns={...})` |
| Referencia a bytes | `zenthros.Artifact(uri, sha256, size)`, la misma forma que reporta un resultado |
| Comandos | `zenthros cache ls|show|rm|sync`, `zenthros cache key export|import`, `zenthros clean --results [--outputs]`, `--cache --older 180d`, `--store 30d` |

El índice son manifiestos firmados en `<store>/cache/<clave>.json`. Solo el SDK los
escribe, y solo después de comprobar el resultado contra los digests de su job. Un
manifiesto sin firma válida no se abre ni protege nada de una limpieza. La firma dice
de quién viene, no que la máquina fuera honesta. Un job que nadie miró entra cuando el
SDK lo vuelve a ver (`get(...).result()`, `tools.jobs`, `zenthros cache sync`).

Con `cache=` cada paso se espera, porque la clave del siguiente sale de lo que dio; sin
él los jobs se encadenan con `dependsOn` y `run` vuelve enseguida. `cache=` es solo de
funciones: un comando no deja un valor que guardar. Por eso `run` de MCP, que manda
comandos, no lo lleva.

Límites: cambiar un archivo del proyecto invalida todos los pasos, porque el proyecto
entero entra a la clave y no se sabe qué lee cada función. Un paso con otro Python menor
no corre como función, porque cloudpickle pide el mismo en los dos lados: `plan` lo dice.
`resume` retoma un paso cortado desde su checkpoint (`spec.resumeFrom`), con la misma
clave y mientras exista el job de origen; si no, corre desde cero y lo dice.

### Del local a otra máquina

| Qué | Cómo |
|---|---|
| Tu entorno en otro lugar | `f.remote(x, on=, env="train")`: lo publicado, por digest; sin publicar y con base pública, esa base con las mismas versiones (`plan` lo dice) |
| Una máquina con su imagen | `create(plantilla, image=)`, y `on=` con `env=` o `image=` la hacen sola: un pool de uno con `image`, `python` y `packages`, sin plantilla nueva. Una VM con containerd y un cluster corren el `image=` de cada job |
| Una máquina que no puede prepararse | Si su Python o sus paquetes fallan (un paquete que reemplazaría a uno de la imagen), el agente lo dice en su latido, y `runtimeFailure` en `GET /machines` lo lee: la llamada termina con esa razón y su máquina se borra, sin esperar el timeout |
| Otra vez, en otro lugar | `call.rerun(on=, resume=, upload=, name=, **opciones)`. `.why` dice por qué no retomó. Los secretos se piden de nuevo |
| Retomar | `spec.resumeFrom: {job, uid}`: nombra un job terminado del namespace, nunca un lugar. El controller lee su último cursor confirmado, y el runner comprueba cada blob contra su digest |
| Subir lo local | `upload=True`, por contenido, a `<store>/data/<sha256>`. Nunca por defecto: sin él, una ruta que ese lugar no ve se frena antes de crear nada |
| Sin bucket | La llamada viaja en los archivos del job, y el resultado en su reporte final, hasta ~189 KiB (`remote/carried.py`). `GET /jobs/{name}/source` devuelve el código con el que corrió, también si la API lo movió a un ConfigMap |
| Vida máxima | `create(plantilla, max_lifetime="24h")`, su default sin `idle=`; `None` la deja hasta borrarla. `spec.scaling.maxLifetime` en el `VMPool`. Razón: `MACHINE_LIFETIME` |
| Sin moverse | `resilient=False`: el job termina con su máquina (`WORKER_LOST`), sin reemplazo |
| Bucket `gs://` | `set_store("gs://...")`: `gcloud` en tu computadora, el rol en una VM de GCP, y en RunPod o Vast la clave de una cuenta hecha para ese prefijo, sellada a la máquina; `set_store(None)` la acaba |

Límites: `rerun` debe poder cargar aquí lo que la llamada nombra por referencia. Un
viewer no puede leer el código de un job que está en un ConfigMap, así que `get()` sin
bucket le falla. Fijar versiones cubre lo que pediste, no lo transitivo, porque el CRD
admite 64 paquetes. La arquitectura se comprueba en plantillas de contenedor.

### `zenthros ui` y el skill

`zenthros ui [--port N] [--no-open]` sirve una página de solo lectura en `127.0.0.1`, con
un token en la dirección que imprime: `/api/targets`, `/api/jobs`, `/api/jobs/NOMBRE`,
`/api/jobs/NOMBRE/logs`, `/api/environments`, `/api/cache`. Responde con `zenthros/tools.py`,
lo mismo que `--json` y MCP, y no guarda nada. Comprueba el `Host` y el token, y lo que
imprime un job llega a la página como texto.

`zenthros agent skill [--to CARPETA]` escribe `<carpeta>/zenthros/SKILL.md`: cuándo usar
Zenthros, cómo leer un resultado, y que permitir lo que cuesta es decisión de la persona.

## 3. Errores: tres dueños, una regla cada uno

| Qué pasó | Quién lo resuelve | Qué ves tú |
|---|---|---|
| Se perdió la máquina (Spot, terminación, agente muerto) | **La plataforma**: reprograma y retoma desde el último checkpoint | nada; `call.info()["reschedules"]` |
| La máquina no arrancó antes de recibir el job (`MACHINE_FAILED`) | **La plataforma**: pide otra máquina; el job nunca corrió, así que `onWorkerLost: Fail` no aplica | nada; cuenta en `call.info()["reschedules"]`, el mismo límite (`workerLostRetryLimit`) |
| Error transitorio **en tu código** (red, 503 de una API) | **El runner**, en el mismo proceso: reintenta solo las excepciones de `retry_on`, hasta `retries` veces, con backoff de 1, 2, 4… s (máximo 30) | nada si al final funciona; `call.attempts` / `info()["attempts"]` |
| Bug en tu código | **Nadie**: falla al primer intento | `RemoteError` con el traceback remoto; `err.__cause__` es tu excepción |
| La plataforma no pudo (timeout, imagen, checkpoint incompleto, máquinas perdidas de más) | el controller | `zenthros.JobFailed` con `.reason`: `DEADLINE_EXCEEDED`, `IMAGE_PULL_FAILED`, `CHECKPOINT_INCOMPLETE`, `WORKER_LOST`, `MACHINE_FAILED` |
| La cancelaste | tú | `errors.JobCancelled` |
| Ctrl-C mientras `result()` espera | **el SDK**: cancela el job y libera la máquina creada para él; con `result(cancel_on_interrupt=False)` lo deja vivo y dice `zenthros.get("<name>")` | `KeyboardInterrupt`; en el registro local, `Interrupted` |
| Tu Python menor no es el de la plantilla | **el SDK**, antes de crear nada: cloudpickle necesita el mismo menor en los dos lados | `ValidationError`; `dry_run` lo pone en `problems` |
| Una función en una plantilla cuyo Python de toolchain no tiene `boto3` (`s3://`) o `cloudpickle` | **el SDK**, antes de crear nada; también con una máquina hecha de esa plantilla (`create`, luego `on=mi`). Visto en Vast.ai el 2026-10-16: "s3:// needs boto3", con la máquina ya pagada | `ValidationError` que dice qué falta: corre un comando ahí (`zenthros.run("python train.py", on=…)`) o el admin añade los paquetes al toolchain. `/machines` da `pythonPackages` |
| Una función en una plantilla sin identidad de nube (Vast.ai, RunPod: `/machines` da `cloudIdentity: false`) | **el SDK**: crea antes la máquina (o usa la de `on=mi`) y le pide al hub credenciales del store selladas para ella, que duran 1 h. Visto en Vast.ai el 2026-10-16: sin ellas, "Unable to locate credentials" | nada en RunPod. En un host de terceros (Vast.ai, `untrustedHost`) se niega antes de crear el job hasta `zenthros.configure(untrusted_host_secrets=True)`. Si no hay máquina arriba o el hub no tiene rol para emitirlas, `ValidationError` antes de la llamada; una máquina hecha para la llamada se borra. Una máquina que la sustituye tiene otra clave y no abre las de la anterior |
| El job ya no existe (lo recogió su TTL, 1 h por defecto o lo que diga `keep_for`, o se borró) | nadie | `errors.JobNotFound`: `wait()` lo espera 15 s (un job recién creado puede no verse aún) y luego termina; `result()` dice que el job fue recogido y dónde quedó el resultado |

**`result()` solo devuelve un valor si el job terminó `Succeeded`.** Antes leía `result.pkl` aunque el job hubiera fallado, y así ocultaba un `CHECKPOINT_INCOMPLETE`.

Tres casos que el runner no reintenta nunca:

- una excepción fuera de `retry_on`;
- un reintento con el aviso de Spot ya encendido: la máquina se va, así que le toca al reemplazo;
- `StoppedOnNotice` / `CheckpointFrozen`: son el traspaso a un reemplazo.

`retries` sin `retry_on` es un error de validación: sin nombrar excepciones, se reintentaría un bug.

Una `Task` reintentada continúa desde su último guardado, no desde cero. El `Checkpointer` recuerda en el proceso el último cursor que el agente confirmó.

En un **comando** no hay excepciones que mirar. Ahí `retries` se traduce en `restartPolicy.backoffLimit`: el controller vuelve a correr el proceso si sale con un código distinto de 0.

---

## 4. Decisiones

1. **`retries` pasó a significar "reintentos de tu código"**, acompañado de `retry_on`.
   - Antes limitaba las reprogramaciones por máquina perdida.
   - Ese límite queda en el default del controller (`workerLostRetryLimit`, 3).
   - Se rompió la API sin alias, como se decidió para el refactor.
2. **La infraestructura va a `configure()`.** Salieron de la firma de `run()`:
   - `store`, `workspace`, `namespace`, `client`, `blob_store`, `ship_sdk` y `python` como default.
   - `run()` recibe solo lo del trabajo.
   - `backend` y `python` siguen en `run()` como override por llamada.
3. **Un `RemoteCall` como argumento es una dependencia.**
   - Se serializa como `Upstream(name)`, y el job recibe `dependsOn` automáticamente.
   - En la máquina, el runner lo cambia por el valor del upstream:
     - el resultado, si es una función;
     - la lista, si es un `map`;
     - la URI de su `output`, si es un comando.
   - No hay scheduler en el cliente: el controller espera, y falla con `DEPENDENCY_FAILED` si el upstream falla.
4. **`map` tiene dos caminos, según dónde se llame.**
   - Desde fuera de un job, es **un** job con `distribution.count = len(items)`. Cada tarea escribe `results/<i>.pkl`.
   - Dentro de un job, sigue siendo fan-out de hijos por el agente, sin pedir otra máquina al API.
   - Es la misma función pública. Dentro de un job pide lo que el fan-out ya pedía:
     - una función importable, no una lambda;
     - items y resultados en JSON.
5. **Un comando no lleva el SDK.**
   - Con el SDK en el tar, `import zenthros` en el script tomaría el core de la llamada en lugar del paquete instalado en la imagen.
   - El comando corre tal cual, con `code_dir` a su lado.
6. **`submit()` acepta solo `zenthros.jobs.spec.Job` tipado.**
   - Se borraron `JobSpec`, el YAML armado a mano y el parseo de tablas.
   - El playground construye su `Job` con los mismos tipos.
7. **Un solo runner de zctl (`zenthros.zctl.Zctl`)** para `jobs` e `infra`, con un solo mapeo de rechazos:
   - `forbidden` → `WorkspaceForbidden`;
   - `invalid`/`usage` → `ValidationError`;
   - cualquier otro → `ZctlError`, con el exit code.
8. **Los secretos solo se referencian.** Pasar un valor en `secrets=` es un error: quedaría en texto plano en el spec. Para lo que no es secreto está `env=`.
9. **Sin kubeconfig, por zenthros-api.** Si hay un `server` (en `configure()`, en `ZENTHROS_SERVER` o en el contexto del zenconfig), el cliente es `HTTPJobClient`:
   - manda el token del usuario, y Kubernetes RBAC decide (`operator/docs/api.md`);
   - `call.pkl` y `result.pkl` viajan por URLs que firma la API (`PresignedBlobStore`), así que el cliente no tiene credenciales de nube;
   - el SHA-256 de la llamada va en el spec (`ZENTHROS_CALL_SHA256`), y el runner rechaza una llamada distinta antes de deserializarla (exit 5);
   - hay un solo cliente por proceso para cada servidor, Workspace, CA y credencial. Es la sesión: `zenthros.session()` la nombra, y `zenthros.workspace()` y `zenthros.login()` devuelven esa misma. Cada hilo reusa su conexión TLS, y un pedido repetible se manda otra vez si el servidor cerró la conexión antes de contestar.
10. **Solo se deserializa lo que escribió la máquina** (auditoría 2026-10-03 SEC-4). Deserializar ejecuta código, y el store lo pueden escribir más que el que llama: otro miembro del Workspace que puede firmar un PUT, un job con las credenciales del bucket, un host de un tercero.
    - El runner guarda `result.pkl` (o `results/<i>.pkl`) y **después** reporta su SHA-256 por el agente, con el productor `zenthros-result-g<generación>[-<i>]`. Solo el job puede reportar por su agente.
    - `result()` compara los bytes con algún digest que el job reportó antes de deserializar. Un resultado sin digest, o con otro, es `RemoteError` y no se lee. El agente guarda el reporte antes de que lo tenga el controller, así que `result()` espera el digest hasta 60 s.
    - `zenthros.get(name)` lee del **job** la URI de la llamada y su digest, y nunca deserializa una llamada con otros bytes. Por eso `get()` necesita que el job exista.
    - Se mantiene cloudpickle: los resultados son objetos Python arbitrarios, y un `Unpickler` restringido rompería a los usuarios. **Un resultado es tan confiable como la máquina que lo produjo.**
    - El resultado de un upstream lo lee la máquina del downstream, que no tiene a quién preguntar el digest. Eso queda dentro de la plataforma: es la misma confianza que la máquina que lo produjo.
11. **La llamada se guarda con su digest como nombre:** `<store>/<name>/call-<sha16>.pkl` (SDK-6). Si un segundo `run(name=...)` se rechaza por nombre repetido, la llamada del job vivo sigue intacta.
12. **El código del proyecto viaja por referencia** (SDK-8). El proyecto de una función se sube una vez a `<store>/code/<sha256>.tar.gz`, y la llamada fija `{uri, sha256}`. El runner lo descarga con las mismas credenciales que la llamada, comprueba el SHA, lo descomprime en un directorio propio del job y corre la función desde ahí, como corría desde la raíz del proyecto. En el spec solo va el core del SDK (~62 KB en base64, sin `infra.py`, `config.py` ni `jobs/fake.py`), que el runner necesita para arrancar. Un comando sigue llevando su código inline, porque corre sin runner.
13. **Qué viaja de `code_dir`** (SDK-5, SDK-9):
    - los `.py`, y los archivos pequeños a su lado (≤ 256 KiB: un `run.sh`, un `conf/train.yaml`);
    - no viaja lo que excluyen `.gitignore` o `.zenthrosignore` (sintaxis de gitignore, con `!` para reincluir), los archivos y directorios ocultos, los entornos (`venv`, `node_modules`, …), ni un `data/` que no sea un paquete de Python;
    - tampoco viajan las credenciales (`*.env`, `*.pem`, `*.key`, `id_rsa*`, `kubeconfig`, …), salvo que un archivo de exclusión las reincluya con `!`: el bundle lo lee quien lea el store;
    - las entradas de `sys.path` que caen dentro de `code_dir` (`python src/train.py` pone `src/` en `sys.path`) viajan como rutas relativas, y el runner las inserta antes de deserializar (SDK-1).
14. **`call.logs(tail=N)`** pide las últimas N líneas al API o a zctl (`--tail`). El `RemoteError` de un comando fallido muestra el final del log, donde está el error. Antes, el log se cortaba desde el principio y el error no aparecía (SDK-10).
15. **El SDK ya no escribe `spec.tenant`**, que está deprecado y el controller ignora (SDK-12).
16. **La API nunca lee el store.** Para listar firma una URL de ListObjectsV2 de un prefijo (método LIST de `objects/presign`, permiso `list zenthrosobjects`), con el prefijo dentro de la firma. El SDK lee las páginas con `after`.
17. **El coste lo cuenta la API, no el SDK.** El SDK no conoce precios ni la máquina de un job. Sin un precio observado, no hay número: nunca se estima.
18. **`idle` es un pool de una máquina con `minReady 0`, `maxReady 1` e `idleTTL`.** Un job a un pool sin `acquire` reutiliza y luego crea, así que la colocación no cambió.
19. **`map` con varios destinos es un job por destino.** Un job de `distribution.count` corre en un solo sitio; repartir es del SDK, que junta los resultados en orden. Los configs van en la llamada (`shared` aparte), no en una función envuelta, porque el bundle registra la función por su módulo.
20. **Sellar en Python es age v1 tal cual** (X25519, HKDF-SHA256, ChaCha20-Poly1305, armor). Un fixture lo fija en los dos lados: el SDK lo escribe byte a byte (`tests/test_seal.py`) y Go lo abre con `filippo.io/age` (`operator/pkg/agepayload`, `TestTheSDKsSealOpensWithAge`). La ruta `/seal` de la API no se usa para esto: vería el valor.
21. **Ctrl-C cancela.** Una celda interrumpida que deja el job corriendo es una máquina que se cobra sin que nadie mire. `result()` y `MapCalls.result()` cancelan y liberan (sin lanzar nada nuevo si eso falla) y vuelven a lanzar el `KeyboardInterrupt`.
22. **Cada llamada deja su registro en disco** (`.zenthros/runs/`, o `configure(log_dir=)` / `ZENTHROS_LOG_DIR`; `off` lo apaga). Es para el usuario y para el asistente que trabaja a su lado: lee archivos, no la pantalla. Nunca guarda valores de secretos ni de `env` (solo nombres), ni la clave. Tiene límites: 10 MiB de log por llamada, 200 llamadas, 2 KiB por argumento. Un disco que falla nunca rompe la llamada: se avisa una vez. La carpeta se ignora a sí misma en git y no viaja con el código. Dentro de un job y con `configure(client=)` (los tests) no escribe nada.
23. **El Python se comprueba antes de crear la máquina.** La API da el Python de cada plantilla (`python` en `/targets` y `/machines`). Un pin sin menor (`3`) o un comando no se comprueban.
24. **`stop_after` lo cumple el controller, no el SDK.** Un notebook cerrado no puede parar nada. Es un campo del Service (`stopAfterSeconds`) y no `activeDeadlineSeconds`: aquel falla el job; este lo da por cumplido (`Cancelled`, `STOP_AFTER`). Cuenta desde el primer arranque, como el muro de un job.
25. **Un puerto de un contenedor de aquí se publica solo en `127.0.0.1`.** Publicar en todas las interfaces abriría el servicio a la red local. La URL que anuncia el agente sale de `ZENTHROS_ENDPOINT_TEMPLATE`, que el SDK pone al crear el contenedor; sin él, el agente anunciaría la IP del contenedor.
26. **Un `map` repartido se recupera por sus jobs.** No hay un objeto "map" en el hub: la etiqueta y las anotaciones de cada job bastan, y los configs salen de la llamada guardada (verificada), no de una anotación que podría no caber.
27. **Un secreto del Workspace lo sella el controller, no el cliente.** Sellar exige la clave de una máquina concreta, y una plantilla la crea después. El valor vive en un Secret de Kubernetes (etiquetado, en el namespace del Workspace), lo escribe zenthros-api y lo lee el controller (sin caché, al mandar el job, para sellarlo), cada uno con un permiso que el Workspace le da solo en su namespace: un RoleBinding a un ClusterRole fijo del chart. El controller solo puede enlazar esos dos ClusterRoles; no tiene Secrets propios en todo el cluster (Kubernetes no deja dar un permiso que no se tiene, y así lo vimos en el E2E). El hub ve el valor, como con `Secret.k8s`; quien no quiera eso sigue teniendo `Secret.encrypted`.
28. **`clean` solo borra llamadas, y por defecto no sus salidas.** Lo que no lleva un `call-<sha16>.pkl` puede ser del usuario, y dentro de una llamada es salida todo lo que no sea la llamada, el resultado o un checkpoint: un modelo no se puede volver a pedir, una llamada sí. El código se queda: pesa KB y se guarda una vez por contenido.

---

## 5. Qué NO está en el SDK, a propósito

| Fuera | Por qué / dónde va |
|---|---|
| Tracking de experimentos, registro de modelos | Tu código los llama dentro del job. `zenthros.report` es para el estado del job. |
| Réplicas, balanceo, endpoint estable (DNS/LB), canary | `serve` publica la URL de una réplica; lo demás es del servidor o de quien lo ponga delante. |
| DSL de pipelines, scheduler, cron, UI de DAGs | Pasar llamadas como argumentos cubre los DAGs chicos. Para algo mayor, Airflow o Prefect llaman a `zenthros.run`. |
| Construir imágenes | `requirements=` arma un venv por función, en caché por hash en la máquina; una imagen propia va en `image=`. |
| Gestión de pools, autoscaling, presupuestos | Es de `zctl` y del operador. El usuario crea y borra sus máquinas de plantilla (`create`/`delete`) y ve lo que costó una llamada (`cost()`), nada más. |
| Guardar secretos | Solo se referencian (AWS SM, k8s). |
| Código de un framework (training, tuning, Ray, polars) | El SDK no importa ninguno: tu función usa el que quiera y guarda sus archivos en el checkpoint (`files=`). |
| Futures u objetos distribuidos | Una llamada como argumento es una dependencia, no un future. |

---

## 6. Casos de uso

**Cubiertos:**

- una función, una `Task` o un comando en otra máquina (`run`, `@cloud`, `.remote/.submit(on=, params=)`);
- elegir dónde y ver antes qué costaría (`targets`, `dry_run`);
- máquinas propias: un contenedor aquí, una VM de plantilla (con `idle`), un pod de un cluster, una máquina que ya existe (`create`, `attach`, `delete`);
- datos: subir, listar, bajar, y leer/escribir por URI igual aquí que en la máquina (`ws.upload/ls/download`, `zenthros.files`);
- muchas variantes, en un sitio o repartidas (`map`, `grid`, `on=[...]`), y en paralelo (`gather`);
- dependencias (`after=`, una llamada como argumento);
- reintentos, `timeout`, máquina perdida y Spot (`resilient`, otras zonas y tipos);
- checkpoints (`Task`, `Checkpointer`, `phase`);
- servicios con URL, desde el notebook (`serve(on=)`, `create("local", ports=)`, `stop_after`, `services()`);
- ver cómo va sin leer el log (`zenthros.report`: la línea en vivo, la columna del `map`, `calls.reports()`);
- que un asistente lea qué falló (`.zenthros/runs/`, `last_failure()`);
- volver a un `map` desde otra sesión (`zenthros.get(name)`) y guardar resultados más de 1 h (`keep_for`);
- secretos sin que el hub vea el valor (`Secret.encrypted`, `s3_secrets`), y para máquinas que aún no existen (`ws.secrets`, `Secret.workspace`);
- borrar llamadas viejas del store (`ws.clean`);
- el entorno por función (`requirements=`, `packages=`, `tools=`);
- lo que corrió, desde otra sesión (`zenthros.get`, `ws.jobs()`) y lo que costó (`cost()`);
- entrenar con cualquier framework y retomar tras perder la máquina, con el modelo como archivo del checkpoint (`Checkpointer.save(state, files=)`; recetas en `examples/checkpoints/`).

**Faltan, útiles para un usuario (propuesta, sin implementar):**

| Caso | Cómo se vería | Qué pide |
|---|---|---|
| Correr una celda sin envolverla en una función | `%%zenthros on="mi-pc"` (magia de IPython): manda la celda con las variables que usa y trae las que asigna | Solo SDK: reutiliza `run` y cloudpickle |
| Volver a lo que corrió ayer | `zenthros.calls(since="1d")`: nombre, estado, duración y coste; `call.download_outputs(to=)` | SDK, sobre `ws.jobs()`, `cost` y `ls` |
| Subir solo lo que cambió | `ws.upload(path, sync=True)`: compara con `ls` y salta lo igual | Solo SDK. `ls` da tamaño, no hash: comparar el hash pide guardarlo como metadato al subir |
| Un presupuesto | `zenthros.configure(budget="$5/day")`: `dry_run` y `create` se niegan si lo pasan | API: la suma de `cost` del Workspace |
| Un Jupyter o un TensorBoard en la GPU | `zenthros.serve("jupyter lab --port 8888", on=…, port=8888)` | En un contenedor de aquí ya funciona (`ports=`). En una plantilla, su pool tiene que exponer el puerto, y la URL sería pública y sin TLS: un túnel solo para el dueño es una decisión de diseño pendiente |
| No volver a bajar los pesos del modelo | caché de Hugging Face que sobreviva entre llamadas | **Medido (E2E `test_workspace_flow`, 2026-10-16):** en una máquina que se queda (`create`), con el runtime de proceso, `~/.cache/huggingface` (`HOME=/home/zenthros`) sigue ahí en la llamada siguiente, sin `HF_HOME`. En una plantilla, 4 llamadas fueron a 4 máquinas distintas: la caché no sirve ahí. Sin medir: un job containerd (`HOME=/tmp` en un contenedor nuevo, `agent/executor.go:305`). Lo que sobreviva a la máquina es un volumen del proveedor |
| Avisar al terminar | `call.on_done(webhook=…)` | Controller: un evento de fin |

## 7. Ejemplo: un modelo de churn cada semana

La tarea tiene cuatro pasos:

1. sacar los datos de la base (con un secreto);
2. entrenar tres variantes en Spot;
3. quedarse con la mejor;
4. mirar el resultado mañana desde otra terminal.

```python
import zenthros

zenthros.configure(store="s3://acme-zenthros/calls", workspace="risk")

# 1. Un script existente, sin tocarlo: escribe en $ZENTHROS_DATA_OUTPUT
prep = zenthros.run(
    ["python", "prep.py", "--month", "2026-09"],
    on="cpu-box", memory="16Gi", image="ghcr.io/acme/churn:3",
    secrets={"DB_URL": zenthros.Secret.aws("prod/warehouse", key="url")},
    retries=3,                 # en un comando: el controller lo re-ejecuta si sale != 0
    timeout="30m",
)


# 2. Entrenar sobreviviendo a Spot
class Train(zenthros.Task):
    policy = zenthros.CheckpointPolicy(every=5)    # cada 5 épocas, y al aviso
    stop_on_notice = True                          # al aviso: guarda y cede la máquina

    def __init__(self, data, depth):
        self.data, self.depth = data, depth

    def setup(self):
        self.model, (self.x, self.y) = build(self.depth), load(self.data)

    def steps(self):
        return 50

    def step(self, epoch):
        zenthros.report({"epoch": epoch, "loss": self.model.fit_one_epoch(self.x, self.y)})

    def state(self):
        return {"weights": self.model.to_b64()}

    def restore(self, s):
        self.model.load_b64(s["weights"])

    def result(self):
        return {"depth": self.depth, "auc": self.model.auc(self.x, self.y)}


data = prep.output                                  # un comando no tiene valor: su salida es una URI
fits = zenthros.map(lambda depth: Train(data, depth).run(), [4, 6, 8],
                    on="aws-spot-gpu", gpu="A10G", spot=True, after=[prep])   # un job, una tarea por depth


# 3. Elegir la mejor: fits es un argumento, así que pick espera a fits y recibe su valor
def pick(results):
    return max(results, key=lambda r: r["auc"])


best = zenthros.run(pick, fits, on="cpu-box", name="churn-2026-09")
print(best.result())   # {'depth': 6, 'auc': 0.91}

# 4. Mañana, desde otra terminal
call = zenthros.get("churn-2026-09")
call.status(), call.info(), call.logs()
```

Si se pierde una máquina Spot, esa variante sigue en otra desde su última época guardada. Un corte de red en `prep.py` hace que el controller lo vuelva a correr. Un bug en `pick` llega como `RemoteError`, con el traceback remoto.

---

## 8. Lo único que pediría Go

Reintentar crashes de proceso (OOM, segfault) de una función sin reintentar sus bugs. Para eso, `processretry.go` tendría que distinguir, por ejemplo, un exit code "permanente".

No lo recomiendo:
- un OOM casi nunca es transitorio: es una señal de que hacen falta más recursos;
- `retry_on` en el runner cubre los transitorios de verdad.
