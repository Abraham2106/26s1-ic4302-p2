# Modelo mongoDB
La base de datos de mongoddb se llama gdelt esta se utiliza unicamente para almacenar los resultados ya procesados. Los datos crudos provenientes de GDELT se mantienen en archivos Parquet para no ser innsertados en MongoDB.

## Colecciones 
Se tiene una coleccion por analisis para poder consultar directamente los resutados sin procesar crudes.

1. heatmap_conflictos
    { 
        "_id": "", 
        "fecha": 20260620, "pais": "", 
        "intensidad_promedio_conflicto": "", 
        "total_eventos_conflicto": "" 
    }
    ### Indice:
        db.heatmap_conflictos.createIndex({ fecha: 1, pais: 1 })
2. top10_paises_eventos_dia
    { 
        "_id": "", 
        "fecha": 20260620, "pais": "", 
        "total_eventos_noticiosos": "", 
        "ranking": "" 
    }
    ### Indice:
        db.top10_paises_eventos_dia.createIndex({ fecha: 1, ranking: 1 })
3. correlacion_avgtone_fuentes
    { 
        "_id": "", 
        "correlacion_avgtone_fuentes": "", 
        "total_eventos_analizados": "", 
        "promedio_avgtone": "", 
        "promedio_fuentes": "" 
    }
    ### Indice:
        db.correlacion_avgtone_fuentes.createIndex({ total_eventos_analizados: 1 })
4. cameo_region_mundo
    { 
        "_id": "", 
        "region_mundo": "", 
        "eventrootcode": "", 
        "total_eventos": "" 
    }
    ### Indice:
        db.cameo_region_mundo.createIndex({ region_mundo: 1, eventrootcode: 1 })
5. matriz_interaccion_actores
    { 
        "_id": "", 
        "actor_origen": "", 
        "actor_destino": "", 
        "frecuencia_interaccion": "" 
    }
    ### Indice:
        db.matriz_interaccion_actores.createIndex({ actor_origen: 1, actor_destino: 1 })
6. cobertura_mediatica_pais
    { 
        "_id": "", 
        "pais": "", 
        "total_eventos": "", 
        "total_menciones": "", 
        "razon_menciones_por_evento": "" 
    }
    ### Indice:
        db.cobertura_mediatica_pais.createIndex({ pais: 1, razon_menciones_por_evento: -1 })
7. tendencia_sentimiento_pais
    { 
        "_id": "", 
        "fecha": "", 
        "pais": "", 
        "avg_tone_diario": "", 
        "total_eventos": "" 
    }  
    ### Indice:
        db.tendencia_sentimiento_pais.createIndex({ pais: 1, fecha: 1 })
8. conflictos_pares_paises
    { 
        "_id": "", 
        "pais_a": "", 
        "pais_b": "", 
        "frecuencia_conflicto": "", 
        "intensidad_promedio_goldstein": "", 
        "mayor_intensidad_conflicto": "" 
    }
    ### Indice:
        db.conflictos_pares_paises.createIndex({ pais_a: 1, pais_b: 1 })
9. escalada_eventos_1h
    { 
        "_id": "", 
        "globaleventid": "", 
        "dia": "", 
        "menciones_24h": "", 
        "menciones_24h_antes": "", 
        "aumento_24h": "", 
        "factor_crecimiento_24h": "" 
    }
    ### Indice:
        db.escalada_eventos_1h.createIndex({ globaleventid: 1, hora: 1 })
10. conflictos_religion_region
    { 
        "_id": "", 
        "region_mundo": "", 
        "religion": "", 
        "total_eventos_conflicto": "", 
        "intensidad_promedio_conflicto": "" 
    }
    ### Indice:
        db.conflictos_religion_region.createIndex({ region_mundo: 1, religion: 1 })
11. top_temas_continente_anio
    { 
        "_id": "", 
        "region_mundo": "", 
        "anio": "", 
        "tema": "", 
        "frecuencia": "", 
        "ranking": "" 
    }
    ### Indice:
        db.top_temas_continente_anio.createIndex({ region_mundo: 1, anio: 1, ranking: 1 })
12. top_organizaciones_global_dia
    { 
        "_id": "", 
        "fecha": "", 
        "organizacion": "", 
        "total_menciones": "", 
        "ranking": "" 
    }
    ### Indice:
        db.top_organizaciones_global_dia.createIndex({ fecha: 1, ranking: 1 })
13. tono_conflicto_ventana
    { 
        "_id": "" , 
        "fecha": "" , 
        "tono_promedio_hoy": "" , 
        "conflictos_hoy": "" , 
        "conflictos_manana": ""  
    }
    ### Indice:
        db.tono_conflicto_ventana.createIndex({ fecha: 1 })
14. correlacion_tono_conflicto_ventana
    { 
        "_id": "", 
        "correlacion_tono_hoy_conflicto_manana": "", 
        "total_datos_analizados": "" 
    }
    ### Indice:
        db.correlacion_tono_conflicto_ventana.createIndex({ total_ventanas_analizadas: 1 }) 2
15. grafo_interacciones_paises
    { 
        "_id": "", 
        "pais_origen": "", 
        "pais_destino": "", 
        "tipo_relacion": "", 
        "peso_arista": "", 
        "goldstein_promedio": "" 
    }
    ### Indice:
        db.grafo_interacciones_paises.createIndex({ pais_origen: 1, pais_destino: 1, tipo_relacion: 1 })
16. diversidad_fuentes_pais
    { 
        "_id": "", 
        "pais": "", 
        "medios_distintos": "", 
        "total_menciones": "", 
        "total_eventos": "", 
        "indice_diversidad_fuentes": "" 
    }
    ### Indice:
        db.diversidad_fuentes_pais.createIndex({ pais: 1, medios_distintos: -1 })
17. frecuencia_conflictos_etnia
    { 
        "_id": "" , 
        "etnia": "", 
        "frecuencia_conflictos": "" , 
        "intensidad_promedio_conflicto": "", 
        "maxima_intensidad_conflicto": ""  
    }
    ### Indice:
        db.frecuencia_conflictos_etnia.createIndex({ etnia: 1, frecuencia_conflictos: -1 })
18. breaking_news_ultima_hora
    { 
        "_id": "" , 
        "globaleventid": "" , 
        "hora": "" , 
        "menciones_en_hora": "" , 
        "menciones_hora_anterior": ""  
    }    
    ### Indice:
        db.breaking_news_ultima_hora.createIndex({ globaleventid: 1, hora: 1 })
19. eventos_positivos_negativos
    { 
        "_id": "", 
        "tipo_eventos": "", 
        "count": "" 
    }
    ### Indice:
        db.eventos_positivos_negativos.createIndex({ tipo_evento: 1 })
20. top10_paises_menos_noticiosos
    { 
        "_id": "", 
        "pais": "", 
        "total_eventos": "", 
        "ranking": ""
    }
    ### Indice:
        db.top10_paises_menos_noticiosos.createIndex({ pais: 1, total_eventos: 1 })

## Indices 
se encuentran en el archivo indexes.js son para acelerar consultas por fecha, país, región, ranking, pares de países y eventos por hora.