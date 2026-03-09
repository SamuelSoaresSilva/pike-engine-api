package com.al_pike_api.data.ingestion.model

data class GeopoliticalEvent(
    val globalEventId: String,
    val date: String,
    val actor1Name: String,
    val actor2Name: String,
    val eventCode: String,
    val goldsteinScale: Double,
    val sourceUrl: String,
)