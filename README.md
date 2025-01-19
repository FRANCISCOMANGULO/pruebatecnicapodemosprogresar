# Prueba Tecnica de Análisis de Eventos Lunares

## Estructura del Proyecto

<pre><div class="relative flex flex-col rounded-lg"><div class="text-text-300 absolute pl-3 pt-2.5 text-xs"></div><div class="pointer-events-none sticky my-0.5 ml-0.5 flex items-center justify-end px-1.5 py-1 mix-blend-luminosity top-0"><div class="from-bg-300/90 to-bg-300/70 pointer-events-auto rounded-md bg-gradient-to-b p-0.5 backdrop-blur-md"><button class="flex flex-row items-center gap-1 rounded-md p-1 py-0.5 text-xs transition-opacity delay-100 hover:bg-bg-200 opacity-60 hover:opacity-100"><svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" fill="currentColor" viewBox="0 0 256 256" class="text-text-500 mr-px -translate-y-[0.5px]"><path d="M200,32H163.74a47.92,47.92,0,0,0-71.48,0H56A16,16,0,0,0,40,48V216a16,16,0,0,0,16,16H200a16,16,0,0,0,16-16V48A16,16,0,0,0,200,32Zm-72,0a32,32,0,0,1,32,32H96A32,32,0,0,1,128,32Zm72,184H56V48H82.75A47.93,47.93,0,0,0,80,64v8a8,8,0,0,0,8,8h80a8,8,0,0,0,8-8V64a47.93,47.93,0,0,0-2.75-16H200Z"></path></svg><span class="text-text-200 pr-0.5">Copy</span></button></div></div><div><div class="code-block__code !my-0 !rounded-lg !text-sm !leading-relaxed"><code><span><span>├── src/               # Código fuente
</span></span><span>├── data/              # Datos simulados y procesados
</span><span>├── datamart/          # Salida de datos en formato Parquet
</span><span>├── docs/              # Documentación y diagramas
</span><span>└── README.md          # Instrucciones y justificación técnica</span></code></div></div></div></pre>

## Flujo de Datos

El proceso de datos sigue el siguiente flujo:

1. **Generación de Datos Simulados**
   * Creación de 50GB de datos simulados usando PySpark
   * Procesamiento por lotes para gestión eficiente de memoria
   * Almacenamiento en formato Parquet
2. **Procesamiento de Datos**
   * Filtrado de eventos lunares
   * Transformaciones y limpieza
   * Cálculo de métricas agregadas
3. **Carga en DataMart**
   * Estructuración según esquema dimensional
   * Particionamiento por año
   * Optimización para consultas analíticas

## Justificación del Diseño Dimensional

### Tabla de Hechos (hechos_eventos)

* **Granularidad** : Un registro por cada evento lunar individual
* **Métricas** : Magnitud, duración, altura
* **Referencias** : Conexiones a todas las dimensiones relevantes

### Dimensiones

1. **dim_tiempo**
   * Jerarquía temporal completa
   * Atributos específicos para análisis lunar
   * Facilita análisis temporales y de ciclos lunares
2. **dim_ubicacion**
   * Jerarquía geográfica
   * Coordenadas precisas
   * Soporte para análisis espaciales
3. **dim_tipo_evento**
   * Categorización detallada
   * Atributos descriptivos
   * Facilita segmentación y filtrado

## Herramientas Propuestas para Producción

### Apache Airflow

* Orquestación de flujos de datos
* Programación de tareas
* Monitoreo y alertas

### Databricks

* Procesamiento distribuido
* Escalabilidad automática
* Integración con Delta Lake

### Delta Lake

* Garantía ACID
* Time travel
* Schema enforcement
* Optimización de rendimiento

## Gestión de Branches

El desarrollo se realiza en tres branches principales:

1. **feature/simulate-data**
   * Código para simulación de datos
   * Generación de dataset de 50GB
2. **feature/process-data**
   * Procesamiento con PySpark
   * Transformaciones y filtros
3. **feature/datamart-design**
   * Diseño del esquema dimensional
   * Documentación técnica
