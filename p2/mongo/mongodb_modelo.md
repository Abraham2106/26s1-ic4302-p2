# Modelo mongoDB
La base de datos usada para el pipeline es MongoDB, esta se utiliza unicamente para almacenar los resultados una vez son procesados. Los datos crudos provenientes de GDELT se mantienen en archivos Parquet durante una hora, pero no son insertados en MongoDB.

## Colecciones 
Se tiene una colección por analisis para poder consultar directamente los resutados sin procesar archivos crudos.

1. heatmap_conflictos
    - _id       ObjectID    
    - fecha     long int    (YYYYMMDD)
    - pais      string      (codigo de dos letras)
    - intensidad_promedio_coflicto      double      
    - total_eventos_conflicto       long 
    ### Indice:
        db.heatmap_conflictos.createIndex({ fecha: 1, pais: 1 })
2. top10_paises_eventos_dia
    - _id       ObjectID
    - fecha     long int    (YYYYMMDD)
    - pais      string      (codigo de dos letras)
    - total_eventos_noticiosos      long 
    - ranking   int
    ### Indice:
        db.top10_paises_eventos_dia.createIndex({ fecha: 1, ranking: 1 })
3. correlacion_avgtone_fuentes
    - _id       ObjectID
    - correlacion_avgtone_fuentes   double
    - total_eventos_analizados      long
    - Promedio_avgtone      double
    - promedio_fuentes      double
    ### Indice:
        db.correlacion_avgtone_fuentes.createIndex({ total_eventos_analizados: 1 })
4. cameo_region_mundo
    - _id       ObjectID
    - region_mundo      string
    - eventrootcode     long string
    - total_eventos     long
    ### Indice:
        db.cameo_region_mundo.createIndex({ region_mundo: 1, eventrootcode: 1 })
5. matriz_interaccion_actores
    - _id       ObjectID
    - actor_origen      string
    - actor_destino     string
    - frecuencia_interaccion        long
    ### Indice:
        db.matriz_interaccion_actores.createIndex({ actor_origen: 1, actor_destino: 1 })
6. cobertura_mediatica_pais
    - _id       ObjectID
    - pais      string
    - total_eventos      long
    - total_menciones    long
    - razon_menciones_por_evento      double
    ### Indice:
        db.cobertura_mediatica_pais.createIndex({ pais: 1, razon_menciones_por_evento: -1 })
7. tendencia_sentimiento_pais
    - _id       ObjectID
    - fecha     long int
    - pais      string
    - avg_tone_diario       double
    - total_eventos     long  
    ### Indice:
        db.tendencia_sentimiento_pais.createIndex({ pais: 1, fecha: 1 })
8. conflictos_pares_paises
    - _id       ObjectID
    - pais_a    string
    - pais_b    string
    - frecuencia_conflicto      long
    - intensidad_promedio_goldstein         double
    - mayor_intensidad_conflicto        double
    ### Indice:
        db.conflictos_pares_paises.createIndex({ pais_a: 1, pais_b: 1 })
9. escalada_eventos_1h
    - _id       ObjectID
    - globaleventid         long
    - hora       date
    - menciones_hora        long
    - menciones_24h_antes       long null
    - aumento_24h       long
    - factor_crecimiento_24h        double
    ### Indice:
        db.escalada_eventos_1h.createIndex({ globaleventid: 1, hora: 1 })
10. conflictos_religion_region
    - _id       ObjectID
    - region_mundo      string
    - religion      string
    - total_eventos_conflicto       long
    - intensidad_promedio_conflicto     double
    ### Indice:
        db.conflictos_religion_region.createIndex({ region_mundo: 1, religion: 1 })
11. top_temas_continente_anio
    - _id       ObjectID
    - region_mundo      string
    - anio      string
    - tema      string
    - frecuencia        long
    - ranking       int
    ### Indice:
        db.top_temas_continente_anio.createIndex({ region_mundo: 1, anio: 1, ranking: 1 })
12. top_organizaciones_global_dia
    - _id       ObjectID
    - fecha     string
    - organizacion      string
    - total_menciones       long
    - ranking       int
    ### Indice:
        db.top_organizaciones_global_dia.createIndex({ fecha: 1, ranking: 1 })
13. tono_conflicto_ventana
    - _id       ObjectID
    - fecha     long int
    - tono_promedio_hoy     double
    - conflictos_hoy        long
    - conflictos_manana     long
    ### Indice:
        db.tono_conflicto_ventana.createIndex({ fecha: 1 })
14. correlacion_tono_conflicto_ventana
    - _id       ObjectID
    - correlacion_tono_hoy_conflicto_manana     double
    - total_datos_analizados        long
    ### Indice:
        db.correlacion_tono_conflicto_ventana.createIndex({ total_ventanas_analizadas: 1 }) 2
15. grafo_interacciones_paises
    - _id       ObjectID
    - pais_origen       string
    - pais_destino      string
    - tipo_relacion     string
    - peso_arista       long
    - goldstein_promedio        double
    ### Indice:
        db.grafo_interacciones_paises.createIndex({ pais_origen: 1, pais_destino: 1, tipo_relacion: 1 })
16. diversidad_fuentes_pais
    - _id       ObjectId
    - pais      string
    - medios_distintos      long
    - total_menciones       long
    - total_eventos     long
    - indice_diversidad_fuentes     double
    ### Indice:
        db.diversidad_fuentes_pais.createIndex({ pais: 1, medios_distintos: -1 })
17. frecuencia_conflictos_etnia
    - _id       ObjectID
    - etnia     string
    - frecuencia_conflictos     long
    - intensidad_promedio_conflicto     double
    - maxima_intensidad_conflicto       double
    ### Indice:
        db.frecuencia_conflictos_etnia.createIndex({ etnia: 1, frecuencia_conflictos: -1 })
18. breaking_news_ultima_hora
    - _id       ObjectID
    - globaleventid     long
    - hora      date
    - menciones_en_hora     long
    - menciones_hora_anterior       long    
    ### Indice:
        db.breaking_news_ultima_hora.createIndex({ globaleventid: 1, hora: 1 })
19. eventos_positivos_negativos
    - _id       ObjectID
    - tipo_eventos      string
    - count     long
    ### Indice:
        db.eventos_positivos_negativos.createIndex({ tipo_evento: 1 })
20. top10_paises_menos_noticiosos
    - _id       Object_id
    - pais      string
    - total_eventos     long
    - ranking       int
    ### Indice:
        db.top10_paises_menos_noticiosos.createIndex({ pais: 1, total_eventos: 1 })

## Indices 
se encuentran en el archivo indexes.js son para acelerar consultas por fecha, país, región, ranking, pares de países y eventos por hora.
