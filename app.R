# -------------------------------------------------
# F1 PERFORMANCE DASHBOARD (Optimized Version)
# -------------------------------------------------

# Load packages ----
library(shiny)
library(shinydashboard)
library(plotly)
library(tidyverse)
library(vroom)

# -------------------------------------------------
# DATA LOADING (with size check + sampling)
# -------------------------------------------------
data_path <- "f1_shiny_subset.csv"

# Safe load with automatic sampling if too large
file_size <- file.info(data_path)$size / (1024^2)  # MB
if (file_size > 500) {
  message("⚠️ Large file detected: sampling 1% for dashboard preview...")
  df <- vroom(data_path, col_types = vroom::cols()) %>%
    sample_frac(0.01)
} else {
  df <- vroom(data_path, col_types = vroom::cols())
}

# Clean and prep
df <- df %>%
  mutate(
    lap_duration = as.numeric(lap_duration),
    rainfall = as.numeric(rainfall),
    humidity = as.numeric(humidity),
    track_temperature = as.numeric(track_temperature),
    air_temperature = as.numeric(air_temperature)
  ) %>%
  drop_na(lap_duration)

# -------------------------------------------------
# DASHBOARD UI ----
# -------------------------------------------------
ui <- dashboardPage(
  dashboardHeader(title = "F1 Performance"),
  dashboardSidebar(
    sidebarMenu(
      menuItem("Lap Performance", tabName = "laps", icon = icon("chart-bar")),
      menuItem("Team Comparison", tabName = "teams", icon = icon("flag-checkered")),
      menuItem("Tyre Strategy", tabName = "tyres", icon = icon("circle")),
      menuItem("Pit Stop Analysis", tabName = "pits", icon = icon("stopwatch")),
      menuItem("Weather Impact", tabName = "weather", icon = icon("cloud-sun"))
    )
  ),
  dashboardBody(
    tabItems(
      # 1️⃣ LAP PERFORMANCE
      tabItem(
        tabName = "laps",
        fluidRow(
          box(width = 12, plotlyOutput("avg_lap_driver")),
          box(width = 6, plotlyOutput("consistent_drivers")),
          box(width = 6, plotlyOutput("lap_by_compound"))
        )
      ),
      
      # 2️⃣ TEAM COMPARISON
      tabItem(
        tabName = "teams",
        fluidRow(
          box(width = 12, plotlyOutput("avg_lap_team")),
          box(width = 12, plotlyOutput("pit_duration_team"))
        )
      ),
      
      # 3️⃣ TYRE STRATEGY
      tabItem(
        tabName = "tyres",
        fluidRow(
          box(width = 12, plotlyOutput("tyre_freq")),
          box(width = 6, plotlyOutput("lap_time_change")),
          box(width = 6, plotlyOutput("position_change"))
        )
      ),
      
      # 4️⃣ PIT STOP ANALYSIS
      tabItem(
        tabName = "pits",
        fluidRow(
          box(width = 12, plotlyOutput("pit_duration_position"))
        )
      ),
      
      # 5️⃣ WEATHER IMPACT
      tabItem(
        tabName = "weather",
        fluidRow(
          box(width = 12, plotlyOutput("lap_temp")),
          box(width = 6, plotlyOutput("lap_rain")),
          box(width = 6, plotlyOutput("lap_humidity"))
        )
      )
    )
  )
)

# -------------------------------------------------
# SERVER ----
# -------------------------------------------------
server <- function(input, output, session) {
  
  # Helper: safely summarize
  safe_mean <- function(x) mean(x, na.rm = TRUE)
  
  # LAP PERFORMANCE ----
  output$avg_lap_driver <- renderPlotly({
    plot_data <- df %>%
      group_by(full_name) %>%
      summarise(avg_lap = safe_mean(lap_duration)) %>%
      arrange(avg_lap) %>%
      head(15)
    
    plot_ly(plot_data, x = ~avg_lap, y = ~reorder(full_name, avg_lap),
            type = "bar", orientation = "h", marker = list(color = "steelblue")) %>%
      layout(title = "Average Lap Duration by Driver",
             xaxis = list(title = "Average Lap Duration (s)"),
             yaxis = list(title = "Driver"))
  })
  
  output$consistent_drivers <- renderPlotly({
    plot_data <- df %>%
      group_by(full_name) %>%
      summarise(std = sd(lap_duration, na.rm = TRUE)) %>%
      arrange(std) %>%
      head(15)
    
    plot_ly(plot_data, x = ~std, y = ~reorder(full_name, std),
            type = "bar", orientation = "h", marker = list(color = "orange")) %>%
      layout(title = "Most Consistent Drivers",
             xaxis = list(title = "Std Dev of Lap Duration (s)"),
             yaxis = list(title = "Driver"))
  })
  
  output$lap_by_compound <- renderPlotly({
    plot_data <- df %>%
      group_by(compound) %>%
      summarise(avg_lap = safe_mean(lap_duration))
    
    plot_ly(plot_data, x = ~compound, y = ~avg_lap, type = "bar",
            marker = list(color = "seagreen")) %>%
      layout(title = "Average Lap Duration by Compound",
             xaxis = list(title = "Compound"),
             yaxis = list(title = "Average Lap Duration (s)"))
  })
  
  # TEAM COMPARISON ----
  output$avg_lap_team <- renderPlotly({
    plot_data <- df %>%
      group_by(team_name) %>%
      summarise(avg_lap = safe_mean(lap_duration)) %>%
      arrange(avg_lap)
    
    plot_ly(plot_data, x = ~avg_lap, y = ~reorder(team_name, avg_lap),
            type = "bar", orientation = "h", marker = list(color = "forestgreen")) %>%
      layout(title = "Average Lap Duration by Team",
             xaxis = list(title = "Mean Lap Duration (s)"),
             yaxis = list(title = "Team"))
  })
  
  output$pit_duration_team <- renderPlotly({
    plot_data <- df %>%
      group_by(team_name) %>%
      summarise(mean_pit = safe_mean(lap_duration[lap_duration > 20 & lap_duration < 60])) %>%
      arrange(mean_pit)
    
    plot_ly(plot_data, x = ~mean_pit, y = ~reorder(team_name, mean_pit),
            type = "bar", orientation = "h", marker = list(color = "tomato")) %>%
      layout(title = "Pit Stop Duration by Team",
             xaxis = list(title = "Mean Pit Duration (s)"),
             yaxis = list(title = "Team"))
  })
  
  # TYRE STRATEGY ----
  output$tyre_freq <- renderPlotly({
    req(df)
    plot_data <- df %>%
      filter(!is.na(compound)) %>%
      count(compound) %>%
      arrange(desc(n))
    
    plot_ly(plot_data, x = ~compound, y = ~n, type = "bar", marker = list(color = "skyblue")) %>%
      layout(title = "Frequency of Tyre Compounds",
             xaxis = list(title = "Tyre Compound"), yaxis = list(title = "Count"))
  })
  
  output$lap_time_change <- renderPlotly({
    plot_data <- df %>%
      group_by(compound) %>%
      summarise(avg = safe_mean(lap_duration))
    
    plot_ly(plot_data, x = ~compound, y = ~avg, type = "bar",
            marker = list(color = "slateblue")) %>%
      layout(title = "Average Lap Duration by Compound",
             xaxis = list(title = "Tyre Compound"),
             yaxis = list(title = "Average Lap Duration (s)"))
  })
  
  output$position_change <- renderPlotly({
    req(df$position)
    plot_data <- df %>%
      group_by(compound) %>%
      summarise(mean_position = safe_mean(position))
    
    plot_ly(plot_data, x = ~compound, y = ~mean_position, type = "bar",
            marker = list(color = "lightcoral")) %>%
      layout(title = "Average Position by Compound",
             xaxis = list(title = "Compound"),
             yaxis = list(title = "Position (Lower = Better)"))
  })
  
  # PIT STOP ANALYSIS ----
  output$pit_duration_position <- renderPlotly({
    plot_data <- df %>%
      filter(lap_duration > 20 & lap_duration < 60) %>%
      group_by(position) %>%
      summarise(mean_pit = safe_mean(lap_duration))
    
    plot_ly(plot_data, x = ~position, y = ~mean_pit, type = "scatter", mode = "markers",
            marker = list(color = "firebrick", size = 10, opacity = 0.7)) %>%
      layout(title = "Average Pit Duration vs Position",
             xaxis = list(title = "Final Position (Lower = Better)"),
             yaxis = list(title = "Average Pit Duration (s)"))
  })
  
  # WEATHER IMPACT ----
  output$lap_temp <- renderPlotly({
    plot_ly(df, x = ~track_temperature, y = ~lap_duration, type = "scatter", mode = "markers",
            marker = list(color = "orange", opacity = 0.6)) %>%
      layout(title = "Lap Duration vs Track Temperature",
             xaxis = list(title = "Track Temperature (°C)"),
             yaxis = list(title = "Lap Duration (s)"))
  })
  
  output$lap_rain <- renderPlotly({
    plot_ly(df, x = ~rainfall, y = ~lap_duration, type = "scatter", mode = "markers",
            marker = list(color = "deepskyblue", opacity = 0.6)) %>%
      layout(title = "Lap Duration vs Rainfall",
             xaxis = list(title = "Rainfall (mm)"),
             yaxis = list(title = "Lap Duration (s)"))
  })
  
  output$lap_humidity <- renderPlotly({
    plot_ly(df, x = ~humidity, y = ~lap_duration, type = "scatter", mode = "markers",
            marker = list(color = "darkcyan", opacity = 0.6)) %>%
      layout(title = "Lap Duration vs Humidity",
             xaxis = list(title = "Humidity (%)"),
             yaxis = list(title = "Lap Duration (s)"))
  })
}

# -------------------------------------------------
# RUN APP ----
# -------------------------------------------------
shinyApp(ui, server)
