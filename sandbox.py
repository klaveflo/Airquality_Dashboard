from shiny import App, render, ui, reactive
import pandas as pd
import requests
import plotly.express as px
from shinywidgets import output_widget, render_widget
import time

# --- 1. User Interface (Frontend) ---
app_ui = ui.page_fluid(
    ui.h2("EEA Air Quality Test-Dashboard 🌍"),
    ui.p("Wähle Parameter und klicke auf den Button, um die API abzufragen."),
    
    ui.row(
        ui.column(4, 
            ui.input_select("country", "Land", {"CH": "Schweiz", "DE": "Deutschland", "FR": "Frankreich"}),
            ui.input_select("pollutant", "Schadstoff", {"NO2": "Stickstoffdioxid", "PM10": "Feinstaub"}),
            # Das ist der wichtigste Teil: Der Action Button!
            ui.input_action_button("fetch_btn", "Daten abrufen & Plotten", class_="btn-primary")
        ),
        ui.column(8,
            ui.output_text("status_message"),
            output_widget("line_plot")
        )
    )
)

# --- 2. Server Logik (Backend) ---
def server(input, output, session):

    # @reactive.event sorgt dafür, dass dieser Code NUR ausgeführt wird, 
    # wenn der Button (fetch_btn) geklickt wird!
    @reactive.Calc
    @reactive.event(input.fetch_btn)
    def get_api_data():
        import time
        start_time = time.time()
        
        api_url = "https://eeadmz1-downloads-api-appservice.azurewebsites.net/ParquetFile/urls"
        
        payload = {
            "countries": [input.country()],
            "cities": [], 
            "pollutants": [input.pollutant()],
            "dataset": 1, 
            "dateTimeStart": "2019-01-01T00:00:00.000Z",
            "dateTimeEnd": "2019-12-31T23:59:59.000Z",
            "compress": False
        }
        
        try:
            print(f"Sende Anfrage an API für {input.country()} und {input.pollutant()}...")
            response = requests.post(api_url, json=payload)
            response.raise_for_status() 
            
            # HIER IST DER FIX: Wir verarbeiten rohen Text statt JSON
            raw_text = response.text
            
            # Text am Zeilenumbruch aufspalten
            lines = raw_text.strip().split('\n')
            
            # Leere Zeilen und die Überschrift herausfiltern
            url_liste = [line.strip() for line in lines if line.strip() and "ParquetFileUrl" not in line]
            
            if not url_liste or len(url_liste) == 0:
                print("Die API hat keine Daten für diese Kombination gefunden.")
                return pd.DataFrame({"Datum": [], "Wert": []}), 0.0
            
            # Wir nehmen für diesen Test einfach die ERSTE Datei aus der Liste
            download_url = url_liste[0]
            print(f"URL erhalten! Lade Datei herunter: {download_url}")
            
            # Pandas lädt das Parquet direkt aus dem Internet
            df = pd.read_parquet(download_url)
            
            end_time = time.time()
            loading_time = round(end_time - start_time, 2)
            
            print("Daten erfolgreich geladen!")
            return df, loading_time

        except Exception as e:
            print(f"Ein Fehler ist aufgetreten: {e}")
            if 'response' in locals():
                print("API Rohtext war:", response.text)
            
            return pd.DataFrame({"Datum": [], "Wert": []}), 0.0

    # Zeigt Statusnachrichten an (Warten, Laden, Fertig)
    @output
    @render.text
    def status_message():
        if input.fetch_btn() == 0:
            return "Bereit. Bitte auf 'Daten abrufen' klicken."
        
        # Wenn der Button geklickt wurde, rufen wir die Daten ab
        df, load_time = get_api_data()
        return f"✅ Daten erfolgreich in {load_time} Sekunden geladen ({len(df)} Zeilen)."

    # Zeichnet den Graphen
    @output
    @render_widget
    def line_plot():
        df, _ = get_api_data()
        
        # Falls leer (Fehler)
        if df.empty:
            import plotly.graph_objects as go
            return go.Figure()

        # HIER DEINE SPALTENNAMEN EINTRAGEN, die du vorhin in der Tabelle gesehen hast!
        # Beispiel: x_spalte = "DatetimeBegin", y_spalte = "Concentration"
        x_spalte = "End" # <-- Anpassen!
        y_spalte = "Value" # <-- Anpassen!
        
        # Zur Sicherheit: Wir stellen sicher, dass die Datumsspalte auch wirklich als Datum erkannt wird
        df[x_spalte] = pd.to_datetime(df[x_spalte])

        # Plotly Graph erstellen
        fig = px.line(
            df, 
            x=x_spalte, 
            y=y_spalte, 
            title=f"Jahresverlauf 2019 für {input.country()} ({input.pollutant()}) - Erste Messstation",
            # Wir machen die Linie etwas dünner, da 8000+ Datenpunkte sonst ein dicker Klumpen sind
            render_mode="webgl" # Nutzt die Grafikkarte des Browsers für bessere Performance bei vielen Punkten
        )
        
        # Ein bisschen hübscher machen
        fig.update_layout(xaxis_title="Datum", yaxis_title="Messwert")
        
        return fig

# App starten
app = App(app_ui, server)