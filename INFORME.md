# Informe - TP Coordinación

## Flujo general

- El recorrido es `Client -> Gateway -> Sum -> Aggregation -> Join -> Gateway -> Client`.
- El Gateway publica los datos en una cola compartida por los Sum.
- Los Sum acumulan cantidades por fruta, los Aggregation calculan tops parciales y el Join arma el top final.
- El protocolo interno usa mensajes `[tipo, clientId, items]`, donde el tipo distingue datos de fin de ingesta.

## Identificación de clientes

- El `messagehandler` asigna un `clientId` numérico a cada conexión mediante un contador atómico.
- El identificador acompaña todos los mensajes internos y permite que varios clientes usen el pipeline al mismo tiempo.
- Sum, Aggregation y Join mantienen el estado separado por `clientId` y lo eliminan cuando termina ese cliente.
- Al volver un resultado, el Gateway lo entrega solamente al handler cuyo `clientId` coincide.

## Coordinación entre Sum

- `input_queue` es una work queue con consumidores en competencia y prefetch 1. Cada mensaje lo procesa un único Sum y cada réplica tiene como máximo un mensaje.
- El Gateway publica un solo EOF por cliente, que también llega a un único Sum. Ese Sum no hace el flush directamente: publica el EOF en el exchange de control con las claves `sum_0..sum_(N-1)`, incluida la propia.
- Cada Sum consume su clave de control en otra goroutine. Al recibir el EOF envía lo acumulado para ese cliente y después un EOF a cada Aggregation.
- Las goroutines de datos y control comparten el mapa de clientes, por lo que el acceso está protegido con un mutex.
- Cada Aggregation espera `SUM_AMOUNT` EOFs por cliente. Como cada Sum envía exactamente uno, la barrera se completa aunque una réplica de sum no haya procesado datos de ese cliente.
- Queda una carrera posible: un Sum puede recibir el control mientras todavía tiene un DATA entregado pero sin procesar. En ese caso hace el flush antes de incorporar el último dato. Para eliminar esa carrera, el EOF original podría incluir la cantidad total de mensajes. Los Sum informarían cuántos procesaron y el Aggregation cerraría la barrera cuando la suma alcance ese total.

## Particionado entre Aggregation

- Cada fruta se asigna a un único Aggregation con `FNV-1a(fruta) % AGGREGATION_AMOUNT`.
- FNV-1a es determinístico. No usamos el hash interno de Go porque puede tener una semilla distinta en cada proceso.
- Cada Sum mantiene un middleware por Aggregation. Los datos de una fruta y el EOF hacia ese destino salen por el mismo canal, por lo que RabbitMQ conserva su orden.
- El EOF se envía a todos los Aggregation, incluso si alguno no recibió frutas. Ese Aggregation produce un top parcial vacío, que el Join cuenta igual.
- Todos los parciales de una fruta llegan al mismo Aggregation. Por eso el Join no vuelve a sumar: une los tops parciales, ordena con `Less` y recorta a `TOP_SIZE`.
- Si una fruta queda fuera del top local, hay al menos `TOP_SIZE` frutas de esa partición por delante. Entonces tampoco puede ser necesaria para el top global. Por eso alcanza con unir los tops parciales.

## Escalado

- Clientes: el estado está separado por `clientId`, por lo que los mensajes de distintos clientes pueden intercalarse sin mezclar resultados.
- Volumen: el Gateway envía un mensaje por línea, pero el estado del Sum depende de la cantidad de frutas distintas del cliente, no de la cantidad de registros recibidos.
- Salida de Sum: se envía un parcial por fruta distinta. Agrupar varios parciales en un mensaje queda pendiente como optimización.
- Cantidad de controles: con N Sum y M Aggregation, por cliente se envían N mensajes de control entre Sum, N x M EOFs hacia los Aggregation, M tops parciales y un resultado final.
- El costo de control crece con la cantidad de réplicas, pero no con la cantidad de líneas del archivo.

## Apagado ordenado

- Sum, Aggregation y Join manejan SIGINT y SIGTERM.
- La señal detiene el consumo principal. `Run` espera el callback en curso y ejecuta el cierre aunque el consumo haya terminado por error.
- Si falla el consumidor de control de un Sum, se detiene también el consumo principal y `Run` devuelve el error.
- Se cierran primero las entradas y después las salidas para no cortar un envío pendiente.
- En el Sum se cierra `inputQueue`, luego el consumidor de control y finalmente los exchanges de salida.

## Limitaciones conocidas

- La coordinación de EOF tiene la carrera descrita anteriormente. El conteo total de mensajes queda como mejora posible.
- Cada Sum abre un middleware por Aggregation. Con N Sum y M Aggregation hay N x M conexiones de datos.
- El hash reparte bien muchas claves, pero con pocas frutas distintas puede dejar carga despareja.
- Gateway envía un mensaje por registro y no se puede agrupar sin cambiar su implementación. El agrupado entre Sum y Aggregation se mantiene pendiente.
