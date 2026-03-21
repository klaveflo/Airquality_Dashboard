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
            "dataset": 2, 
            "dateTimeStart": "2019-01-01T00:00:00.000Z", # Wird von der API teils ignoriert
            "dateTimeEnd": "2019-12-31T23:59:59.000Z",   # Wird von der API teils ignoriert
            "compress": False
        }
        
        try:
            print(f"Sende Anfrage an API für {input.country()} und {input.pollutant()}...")
            response = requests.post(api_url, json=payload)
            response.raise_for_status() 
            
            raw_text = response.text
            lines = raw_text.strip().split('\n')
            url_liste = [line.strip() for line in lines if line.strip() and "ParquetFileUrl" not in line]
            
            if not url_liste or len(url_liste) == 0:
                print("Die API hat keine Daten für diese Kombination gefunden.")
                return pd.DataFrame({"Datum": [], "Wert": []}), 0.0
            
            download_url = url_liste[0]
            print(f"URL erhalten! Lade komplette Datei herunter: {download_url}")
            
            df = pd.read_parquet(download_url)
            
            # === AB HIER STARTET DIE DATENBEREINIGUNG (DATA CLEANING) ===
            zeit_spalte = "Start" 
            wert_spalte = "Value"
            
            # 1. Datum aus den Nanosekunden in Pandas-Datetime umwandeln
            if pd.api.types.is_numeric_dtype(df[zeit_spalte]):
                df[zeit_spalte] = pd.to_datetime(df[zeit_spalte], unit='ns', errors='coerce')
            else:
                df[zeit_spalte] = pd.to_datetime(df[zeit_spalte], errors='coerce')
            
            # 2. LOKALER FILTER: Da die API uns oft die Jahre 2017-2024 schickt, 
            # filtern wir das DataFrame jetzt hier lokal auf das gewünschte Jahr 2019!
            start_datum = "2019-01-01"
            end_datum = "2019-12-31"
            maske = (df[zeit_spalte] >= start_datum) & (df[zeit_spalte] <= end_datum)
            df = df.loc[maske].copy() # .copy() verhindert Warnungen im nächsten Schritt
            
            # 3. DATUM FÜR PLOTLY REPARIEREN:
            # Wir erstellen eine neue Spalte, in der das Datum als reiner Text steht ("YYYY-MM-DD HH:MM")
            # Damit kann Plotly garantiert umgehen!
            df["Datum_Plot"] = df[zeit_spalte].dt.strftime("%Y-%m-%d %H:%M")
            
            # 4. Fehlwerte (-999) entfernen
            df = df[df[wert_spalte] >= 0]
            
            end_time = time.time()
            loading_time = round(end_time - start_time, 2)
            
            print(f"Daten erfolgreich gereinigt! Es bleiben {len(df)} Zeilen für das Jahr 2019.")
            return df, loading_time

        except Exception as e:
            print(f"Ein Fehler ist aufgetreten: {e}")
            return pd.DataFrame({"Datum_Plot": [], "Value": []}), 0.0

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
        
        if df.empty:
            import plotly.graph_objects as go
            return go.Figure()

        # ACHTUNG: Wir nutzen jetzt unsere kugelsichere Text-Datumsspalte für die X-Achse!
        x_spalte = "Datum_Plot" 
        y_spalte = "Value" 
        
        # Plotly Graph erstellen
        fig = px.line(
            df, 
            x=x_spalte, 
            y=y_spalte, 
            title=f"Jahresverlauf 2019 für {input.country()} ({input.pollutant()})"
        )
        
        # Da wir auf ein Jahr gefiltert haben, sind es unter 10.000 Punkte.
        # Das schafft Plotly problemlos ohne webgl.
        fig.update_layout(
            xaxis_title="Datum", 
            yaxis_title="Messwert (µg/m³)",
            xaxis=dict(type='category') # Zwingt Plotly, unsere Text-Daten exakt so zu belassen
        )
        
        # Wichtig: Bei vielen Datenpunkten (wie stündlichen Messungen über ein Jahr) 
        # würden sonst Tausende Labels auf der X-Achse kleben. Wir verstecken die meisten:
        fig.update_xaxes(nticks=12) 
        
        return fig

# App starten
app = App(app_ui, server)