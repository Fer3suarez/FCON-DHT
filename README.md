# FCON-DHT

## DESARROLLAR UNA APLICACIÓN DE UNA DHT (DISTRIBUTED HASH TABLE) 
 
 ### Objetivos
* Desarrollar una aplicación distribuida de una DHT
* Propiedades de la aplicación: Coherencia y tolerancia de fallos

### Introducción
El objetivo de esta práctica es desarrollar un aplicación distribuida que gestione una DHT (Distributed hash Table) y que cumple las propiedades como coherencia entre los servidores, tolerancia de fallos, transparencia y disponibilidad. El interfaz de usuario podrá ser muy sencillo, para que el esfuerzo se concentra en las propiedades distribuidas.
Las tablas de hash es un algoritmo de diccionarios, que permite gestionar los datos con muy buen rendimiento. Esta tabla estructura los datos como pares <clave, valor>. A partir de la clave, se genera un identificador mediante una función hash.
Este parámetro indica la posición del valor en la tabla. La tablas DHT es una extensión sobre las tablas hash, en las que no están centralizadas. La tabla global se divida en particiones y cada una se almacena en diferentes servidores o nodos de la aplicación distribuida. El interfaz debería ser exactamente que una tabla centralizada. El usuario no debería distinguir entre un sistema distribuido o centralizado. Además, el comportamiento externo en estas implementaciones debería ser igual.
Los servidores deberán gestionar una partición de la tabla global. En concreto, los identificadores se deberán mantener en un rango concreto de los valores en el
espacio de la función hash. En esta práctica, el tamaño de lo rangos de cada nodo será igual. Los servirán mantendrán la información que relacione las particiones de
los datos con la dirección IP del servidor que gestiona la correspondiente tabla. De esta forma, cuando un servidor reciba una invocación de un usuario, la redirigirá al
servidor correspondiente.
En esta implementación los pares de la tabla será: <String, Integer>, donde el primer valor es la clave y el segundo el valor. El interfaz básico consiste las siguientes
operaciones, que son similares a las declaradas en un interfaz un Map en Java: 
* Integer put(map): Map es una estructura de datos con los valores de los pares mencionados. El resultado será el valor o un null si no se puede guardar el valor.
* Integer get(key): La respuesta será el valor asociado a la clave o un null si la clave no existe.
* Integer remove(key): La respuesta será el valor asociado a la clave o un null si la clave no existe o no se ha podido borrar..

### Tolerancia de los fallos
La tolerancia de los fallos permite ocultar los fallos del sistema, que no se pierdan datos y que el usuario no detecten de los fallos. Para ello, cada una partición se
replicará en varios servidores. Por ejemplo, supóngase que se tiene un sistema con cuatro servidores, con una implementación de 3 fallos, como podría ser en la
siguiente estructura:

| Tabla 0 | Tabla 1 | Tabla 2 | 
| ------------- | ------------- | ------------- |
| Servidor 0  | Servidor 1  | Servidor 2  |
| Servidor 2  | Servidor 0  | Servidor 1  |

En el caso de un fallo, el sistema deberá detectar este evento. En el momento en el que se añade un servidor, habría que integrarlo en el sistema, transfiriendo las tablas requeridas.

### Coherencia
Las tablas de los servidores replicados deberán tener los mismos valores. El usuario debería obtener el mismo valor, independientemente del servidor al que se refiera.

### Implementación 

La coherencia y tolerancia de fallos se basara en la herramienta ZooKeeper.

## Requisitos
1. La aplicación distribuida gestionará los datos de una tabla hash distribuida. Los pares del diccionario serán: <String, Integer>
2. Al menos, la aplicación podrá proporcionar las funciones: put, get y remove.
3. La aplicación podría disponer un interfaz de usuario textual.
4. La aplicación dispondrá de un conjunto de servidores replicados para satisfacer las características distribuidas requeridas: tolerancia de fallos, coherencia, transparencia y disponibilidad
5. Coherencia: el estado de las tablas replicadas deberá ser coherente y el usuario no podrá distinguir los resultados retornados de diferentes servidores.
6. Tolerancia de fallos: La aplicación tolerará un número de fallos (al menos, uno). La funcionalidad será la misma aunque ocurran el número de datos determinados.
La aplicación dispondrá de un número inicial de los servidores correctos. En el caso de que un servidor se caiga, se deberá crear un servidor nuevo para mantel
en número de fallos requeridos. Al crear al nuevo servidor, habrá que transferir el mismo el estado actualizado de las tablas asociadas.
7. Disponibilidad: Los servidores podrán invocar a cualquier servidor. Serían aceptables los retardos en la operación motivada por la gestión de datos de fallos.
8. Los znodes en un ensemble ZooKeeper no podrá almacenar las tablas de los servidores. Las tablas se deberán almacenar en los servidores y las funciones de
ZooKeeper servirán para coordinar la aplicación distribuida y garantizar la tolerancia de fallos y coherencia. Hay que recordar que ZooKeeper no es una base de datos.
9. La coherencia de los datos de los servidores se deberá gestionar basando en ZooKeeper. La excepción es cuando haya que transferir datos a un nuevo servidor integrado. La coordinación en este caso, se usará ZooKeeper antes de enviar los datos. Esta herramienta garantiza la fiabilidad de la comunicación. Si se usará una
alternativa habría que usar un protocolo de comunicación alternativo y que, probablemente, aumentaría la complejidad y rendimiento del sistema.

## Diagrama de clases

![Diagrama de clases](https://github.com/Fer3suarez/FCON-DHT/blob/master/DHT_2020/src/es/upm/dit/dscc/DHT/diagrama-DHT.jpg)

## Pasos para la ejecución

1. Clonar repositorio
2. Crear ensenble Zookeeper
```
./zkServer.sh start ~/zookeeper/standalone.cfg
```
3. Importar proyecto en eclipse con las librerias adecuadas
   - Java 11
   - Zookeeper
4. Crear tres servidores para conseguir Quorum

## Interfaz de usuario

| **NUMBER** | 1 | 2 | 3 | 4 | 5 | 6 | 0 |
| --- | --- | --- | --- | ---- | --- | --- | --- |
|  **OPERATION** | put | get | remove | contains | show tables | init | exit |

> [Solución](https://github.com/Fer3suarez/FCON-DHT/tree/master/DHT_2020/src/es/upm/dit/dscc/DHT)
