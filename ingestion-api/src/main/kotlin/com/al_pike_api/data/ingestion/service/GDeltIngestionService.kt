package com.al_pike_api.data.ingestion.service

import com.al_pike_api.data.ingestion.model.GeopoliticalEvent
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Service
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.zip.ZipInputStream

@Service
class GdeltIngestionService {

    private val httpClient = HttpClient.newHttpClient()
    private val lastUpdateUrl = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"

    @EventListener(ApplicationReadyEvent::class)
    fun testRunOnStartup() {
        println("Testando a ingestão do GDELT na inicialização...")
        fetchLatestGeopoliticalEvents()
    }

    fun fetchLatestGeopoliticalEvents() {
        println("Iniciando busca de atualizações do GDELT...")

        // 1. Busca o arquivo de texto que contém a URL da última atualização
        val request = HttpRequest.newBuilder()
            .uri(URI.create(lastUpdateUrl))
            .GET()
            .build()

        val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
        val lastUpdateData = response.body()

        // O arquivo tem 3 linhas (Export, Mentions, GKG). Queremos o 'export' (Eventos)
        val exportLine = lastUpdateData.lines().firstOrNull { it.contains("export.CSV.zip") }

        // Separa a linha por espaços e pega o último item (que é a URL do ZIP)
        val zipUrl = exportLine?.split("\\s+".toRegex())?.lastOrNull()

        if (zipUrl != null) {
            println("URL do último ZIP encontrada: $zipUrl")
            downloadAndParseZip(zipUrl)
        } else {
            println("Nenhuma URL de exportação encontrada no momento.")
        }
    }
    private fun downloadAndParseZip(zipUrl: String) {
        val request = HttpRequest.newBuilder().uri(URI.create(zipUrl)).GET().build()
        val response = httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream())

        // 2. Extrai o ZIP direto da stream de resposta
        ZipInputStream(response.body()).use { zipInputStream ->
            val entry = zipInputStream.nextEntry

            if (entry != null && entry.name.endsWith(".csv")) {
                println("Lendo arquivo CSV: ${entry.name}")

                // 👇 ESTA É A LINHA QUE ESTAVA FALTANDO! Ela cria o 'reader'
                val reader = BufferedReader(InputStreamReader(zipInputStream))

                // 3. Processa as linhas do CSV e converte para objetos
                val events = reader.useLines { lines ->
                    lines.mapNotNull { line ->
                        val cols = line.split("\t")

                        // Garante que a linha tem colunas suficientes
                        if (cols.size >= 60) {
                            GeopoliticalEvent(
                                globalEventId = cols[0],
                                date = cols[1],
                                actor1Name = cols[6],
                                actor2Name = cols[16],
                                eventCode = cols[26],
                                goldsteinScale = cols[30].toDoubleOrNull() ?: 0.0,
                                sourceUrl = cols[60]
                            )
                        } else {
                            null
                        }
                    }.toList()
                }

                println("🛰️ Sucesso! ${events.size} eventos geopolíticos capturados e estruturados.")

                // Aqui você tem a lista pronta na variável 'events'
            }
        }
    }
}