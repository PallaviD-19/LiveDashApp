import os
import csv
import time
import pandas as pd
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
from threading import Thread

# Folder path and output CSV
folder_path = 'processed'  # Update this path with the folder containing log files
output_csv = 'extracted_data.csv'

# Function to process each file and extract data
def process_log_file(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()
        if len(lines) >= 2:
            headers = lines[0].strip().split('~')  # First line contains headers
            values = lines[1].strip().split('~')   # Second line contains values
            values = [val if val != '' else None for val in values]  # Handle empty values
            return headers, values
    return None, None

# Function to extract data to CSV
def extract_data_to_csv(folder_path, output_csv):
    with open(output_csv, 'w', newline='') as csv_file:
        writer = None
        for filename in os.listdir(folder_path):
            if filename.endswith('.log') or filename.endswith('.txt'):
                file_path = os.path.join(folder_path, filename)
                headers, values = process_log_file(file_path)
                if headers and values:
                    if writer is None:
                        writer = csv.writer(csv_file)
                        writer.writerow(headers)  # Write headers once
                    writer.writerow(values)  # Write row values
    print(f'Data extracted and saved to {output_csv}')

# Function to monitor the folder and update CSV when files are added/removed
def monitor_and_update():
    last_modified = None
    while True:
        current_modified = os.path.getmtime(folder_path)
        if last_modified is None or current_modified != last_modified:
            extract_data_to_csv(folder_path, output_csv)  # Regenerate CSV
            last_modified = current_modified
        time.sleep(5)  # Check every 5 seconds

# Dash App Setup
app = dash.Dash(__name__, suppress_callback_exceptions=True)
server = app.server

# df = pd.read_csv(output_csv)
# Check if the file exists before loading it
if os.path.exists(output_csv):
    df = pd.read_csv(output_csv)
    # Convert the 'CREATION_DATE' to datetime format
    df['CREATION_DATE'] = pd.to_datetime(df['CREATION_DATE'], dayfirst=True, errors='coerce')
else:
    # Create an empty DataFrame with the expected structure if the file doesn't exist
    df = pd.DataFrame(columns=['APPLICATION_ID', 'FUNCTIONALITY', 'GROUP_ID', 'REQUEST_STATUS', 'CREATION_DATE', 'SITE_ID'])

# Convert the 'CREATION_DATE' to datetime format
df['CREATION_DATE'] = pd.to_datetime(df['CREATION_DATE'], dayfirst=True, errors='coerce')

# List of columns for value counts (only specific columns)
value_count_columns = ['APPLICATION_ID', 'FUNCTIONALITY', 'GROUP_ID', 'REQUEST_STATUS', 'SITE_ID']

# Layout with Tabs, Dropdowns on the left, and Graph/Table on the right
app.layout = html.Div(
    style={'backgroundColor': '#e8f4f8', 'padding': '20px', 'fontFamily': 'Verdana, sans-serif'},
    children=[

        html.H1("Application Requests Dashboard", style={'textAlign': 'center', 'color': '#1f4e79', 'marginBottom': '40px'}),

        dcc.Tabs([
            dcc.Tab(label='Requests Per Day', children=[
                html.Div([

                    # Left-side Filters
                    html.Div([
                        html.Label("Select Application ID", style={'fontWeight': 'bold'}),
                        dcc.Dropdown(
                            id='application_id_filter',
                            options=[{'label': app_id, 'value': app_id} for app_id in df['APPLICATION_ID'].unique()],
                            value=None,
                            placeholder="Select an Application ID",
                            style={'width': '80%', 'fontSize': '14px'}
                        ),
                        html.Br(),

                        html.Label("Select Functionality", style={'fontWeight': 'bold'}),
                        dcc.Dropdown(
                            id='functionality_filter',
                            options=[],
                            value=None,
                            placeholder="Select a Functionality",
                            style={'width': '80%', 'fontSize': '14px'}
                        ),
                        html.Br(),

                        html.Label("Select Group ID", style={'fontWeight': 'bold'}),
                        dcc.Dropdown(
                            id='group_id_filter',
                            options=[],
                            value=None,
                            placeholder="Select a Group ID",
                            style={'width': '80%', 'fontSize': '14px'}
                        ),
                        html.Br(),

                        html.Label("Select Request Status", style={'fontWeight': 'bold'}),
                        dcc.Dropdown(
                            id='request_status_filter',
                            options=[{'label': status, 'value': status} for status in df['REQUEST_STATUS'].unique()],
                            value=None,
                            placeholder="Select a Request Status",
                            style={'width': '80%', 'fontSize': '14px'}
                        ),
                        html.Br(),

                        html.Label("Select Date Range", style={'fontWeight': 'bold'}),
                        dcc.DatePickerRange(
                            id='date_range_picker',
                            start_date=df['CREATION_DATE'].min(),
                            end_date=df['CREATION_DATE'].max(),
                            display_format='DD-MM-YYYY',
                            style={'width': '80%','fontSize': '14px'}
                        )
                    ], style={'width': '25%', 'display': 'inline-block', 'verticalAlign': 'top', 'paddingRight': '20px'}),

                    # Right-side Graph and Table
                    html.Div([
                        # Graph
                        dcc.Graph(id='requests_per_day_graph'),

                        # Data Table
                        html.Div(id='table_data', style={'marginTop': '20px', 'overflowY': 'auto', 'maxHeight': '200px'})
                    ], style={'width': '73%', 'display': 'inline-block'})
                ])
            ]),

            dcc.Tab(label='Overall Frequency Counts', children=[
                html.Div([

                    # Left-side Filters
                    html.Div([
                        html.Label("Select a Column", style={'fontWeight': 'bold'}),
                        dcc.Dropdown(
                            id='column_selection_dropdown',
                            options=[{'label': col, 'value': col} for col in value_count_columns],
                            value=None,
                            placeholder="Select a Column",
                            style={'width': '80%', 'fontSize': '14px'}
                        )
                    ], style={'width': '25%', 'display': 'inline-block', 'verticalAlign': 'top', 'paddingRight': '20px'}),

                    # Right-side Graph and Table
                    html.Div([
                        # Graph for value counts
                        dcc.Graph(id='value_counts_graph'),

                        # Data Table for value counts
                        html.Div(id='value_counts_table', style={'marginTop': '20px', 'overflowY': 'auto', 'maxHeight': '200px'})
                    ], style={'width': '73%', 'display': 'inline-block'})
                ])
            ])
        ])
    ])

# Callback to update the Functionality dropdown based on Application ID
@app.callback(
    [Output('functionality_filter', 'options'), Output('functionality_filter', 'value')],
    Input('application_id_filter', 'value')
)
def set_functionality_options(selected_application_id):
     if os.path.exists(output_csv):
        df = pd.read_csv(output_csv)
        # Convert the 'CREATION_DATE' to datetime format
        df['CREATION_DATE'] = pd.to_datetime(df['CREATION_DATE'], dayfirst=True, errors='coerce')
    
        if selected_application_id:
            filtered_df = df[df['APPLICATION_ID'] == selected_application_id]
            functionalities = [{'label': func, 'value': func} for func in filtered_df['FUNCTIONALITY'].unique()]
        else:
            functionalities = []
        return functionalities, None

# Callback to update the Group ID dropdown based on Application ID and/or Functionality
@app.callback(
    [Output('group_id_filter', 'options'), Output('group_id_filter', 'value')],
    [Input('application_id_filter', 'value'), Input('functionality_filter', 'value')]
)
def set_group_id_options(selected_application_id, selected_functionality):
     if os.path.exists(output_csv):
        df = pd.read_csv(output_csv)
        # Convert the 'CREATION_DATE' to datetime format
        df['CREATION_DATE'] = pd.to_datetime(df['CREATION_DATE'], dayfirst=True, errors='coerce')

        filtered_df = df.copy()

        if selected_application_id and selected_functionality:
            filtered_df = filtered_df[
                (filtered_df['APPLICATION_ID'] == selected_application_id) &
                (filtered_df['FUNCTIONALITY'] == selected_functionality)
            ]
        elif selected_application_id:
            filtered_df = filtered_df[filtered_df['APPLICATION_ID'] == selected_application_id]
        elif selected_functionality:
            filtered_df = filtered_df[filtered_df['FUNCTIONALITY'] == selected_functionality]

        group_ids = [{'label': group_id, 'value': group_id} for group_id in filtered_df['GROUP_ID'].unique()]
        return group_ids, None

# Callback to update the graph and table based on filters
@app.callback(
    [Output('requests_per_day_graph', 'figure'), Output('table_data', 'children')],
    [Input('application_id_filter', 'value'),
     Input('functionality_filter', 'value'),
     Input('group_id_filter', 'value'),
     Input('request_status_filter', 'value'),
     Input('date_range_picker', 'start_date'),
     Input('date_range_picker', 'end_date')]
)

def update_graph_and_table(selected_application_id, selected_functionality, selected_group_id, selected_request_status, start_date, end_date):
     if os.path.exists(output_csv):
        df = pd.read_csv(output_csv)
        # Convert the 'CREATION_DATE' to datetime format
        df['CREATION_DATE'] = pd.to_datetime(df['CREATION_DATE'], dayfirst=True, errors='coerce')

        # Load the dataframe (assuming df is the original dataframe you're filtering)
        filtered_df = df.copy()  # Initialize the dataframe
        
        # Convert start_date and end_date to datetime if they are not None
        if start_date is not None:
            start_date = pd.to_datetime(start_date)
        if end_date is not None:
            end_date = pd.to_datetime(end_date)
        
        # Filter based on date range if both start_date and end_date are provided
        if start_date is not None and end_date is not None:
            filtered_df = filtered_df[
                (filtered_df['CREATION_DATE'] >= start_date) &
                (filtered_df['CREATION_DATE'] <= end_date)
            ]
        
        # Filter based on other dropdowns if selections are made
        if selected_application_id is not None:
            filtered_df = filtered_df[filtered_df['APPLICATION_ID'] == selected_application_id]
        
        if selected_functionality is not None:
            filtered_df = filtered_df[filtered_df['FUNCTIONALITY'] == selected_functionality]
        
        if selected_group_id is not None:
            filtered_df = filtered_df[filtered_df['GROUP_ID'] == selected_group_id]
        
        if selected_request_status is not None:
            filtered_df = filtered_df[filtered_df['REQUEST_STATUS'] == selected_request_status]
    
        # Create the figure (assuming you're using Plotly for visualization)
        fig = create_plotly_figure(filtered_df)  # Your figure generation function
        
        # Create the table (assuming you're using Dash DataTable or similar)
        table = create_dash_table(filtered_df)  # Your table generation function
        
        return fig, table

# Callback to update the value counts graph and table based on column selection
@app.callback(
    [Output('value_counts_graph', 'figure'), Output('value_counts_table', 'children')],
    Input('column_selection_dropdown', 'value')
)
def update_value_counts_graph_and_table(selected_column):
     if os.path.exists(output_csv):
        df = pd.read_csv(output_csv)
        # Convert the 'CREATION_DATE' to datetime format
        df['CREATION_DATE'] = pd.to_datetime(df['CREATION_DATE'], dayfirst=True, errors='coerce')

        if selected_column:
            value_counts = df[selected_column].value_counts().reset_index()
            value_counts.columns = [selected_column, 'Count']

            # Create colorful graph
            fig = px.bar(
                value_counts, 
                x=selected_column, 
                y='Count', 
                color='Count',
                color_continuous_scale='Plasma',  # Add color for value counts
                title=f'Frequency Counts for {selected_column}'
            )

            # Create smaller table
            table = html.Table([
                html.Thead(html.Tr([html.Th(col) for col in value_counts.columns], style={'fontSize': '12px'})),
                html.Tbody([
                    html.Tr([html.Td(value_counts.iloc[i][col], style={'fontSize': '12px'}) for col in value_counts.columns]) 
                    for i in range(min(len(value_counts),100))  # Limiting to 10 rows for better display
                ])
            ])

            return fig, table
        else:
            return {}, html.Div()


# Thread to monitor the folder for changes
def start_folder_monitoring():
    monitor_thread = Thread(target=monitor_and_update)
    monitor_thread.daemon = True
    monitor_thread.start()

# Start the folder monitoring thread
start_folder_monitoring()

import sys

if __name__ == '__main__' and not sys.argv[0].endswith('ipykernel_launcher.py'):
    app.run_server(debug=True)
