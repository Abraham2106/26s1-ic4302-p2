use("gdelt");
// 1
db.heatmap_conflictos.createIndex({ fecha: 1, pais: 1 })

// 2
db.top10_paises_eventos_dia.createIndex({ fecha: 1, ranking: 1 })

// 3
db.correlacion_avgtone_fuentes.createIndex({ total_eventos_analizados: 1 })

// 4
db.cameo_region_mundo.createIndex({ region_mundo: 1, eventrootcode: 1 })

// 5
db.matriz_interaccion_actores.createIndex({ actor_origen: 1, actor_destino: 1 })

// 6
db.cobertura_mediatica_pais.createIndex({ pais: 1, razon_menciones_por_evento: -1 })

// 7
db.tendencia_sentimiento_pais.createIndex({ pais: 1, fecha: 1 })

// 8
db.conflictos_pares_paises.createIndex({ pais_a: 1, pais_b: 1 })

// 9
db.escalada_eventos_1h.createIndex({ globaleventid: 1, hora: 1 })

// 10
db.conflictos_religion_region.createIndex({ region_mundo: 1, religion: 1 })

// 11
db.top_temas_continente_anio.createIndex({ region_mundo: 1, anio: 1, ranking: 1 })

// 12
db.top_organizaciones_global_dia.createIndex({ fecha: 1, ranking: 1 })

// 13
db.tono_conflicto_ventana.createIndex({ fecha: 1 })
db.correlacion_tono_conflicto_ventana.createIndex({ total_ventanas_analizadas: 1 })

// 14
db.grafo_interacciones_paises.createIndex({ pais_origen: 1, pais_destino: 1, tipo_relacion: 1 })

// 15
db.diversidad_fuentes_pais.createIndex({ pais: 1, medios_distintos: -1 })

// 16
db.frecuencia_conflictos_etnia.createIndex({ etnia: 1, frecuencia_conflictos: -1 })

// 17
db.breaking_news_ultima_hora.createIndex({ globaleventid: 1, hora: 1 })

// Extra 1
db.eventos_positivos_negativos.createIndex({ tipo_evento: 1 })

// Extra 2
db.top10_paises_menos_noticiosos.createIndex({ pais: 1, total_eventos: 1 })