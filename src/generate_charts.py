import os
import pandas as pd
import matplotlib.pyplot as plt

# Configuration
RESULTS_DIR = 'results'
CHARTS_DIR = 'charts'
SIZES = ['small', 'medium', 'big']
INDEX_STATES = ['not_indexed', 'indexed']
SCALES = ['linear', 'log']

# Ensure output directory exists
os.makedirs(CHARTS_DIR, exist_ok=True)
os.makedirs(os.path.join(CHARTS_DIR, "benchmarks"), exist_ok=True)
os.makedirs(os.path.join(CHARTS_DIR, "scale_trends"), exist_ok=True)

# Consistent colors for databases
DB_COLORS = {
    'postgres': '#3366CC',  # Blue
    'mysql': '#FF9900',     # Orange
    'mongodb': '#109618',   # Green
    'neo4j': '#DC3912'      # Red
}

DB_ORDER = ['postgres', 'mysql', 'mongodb', 'neo4j']

def plot_benchmark_data(df, size, index_state, scale):
    """
    Generates a 2x2 grid of subplots for CRUD operations from the dataframe.
    """
    fig, axes = plt.subplots(2, 2, figsize=(18, 14))
    
    # Title generation
    idx_title = "Bez indeksów" if index_state == 'not_indexed' else "Z indeksami"
    scale_title = "Skala liniowa" if scale == 'linear' else "Skala logarytmiczna"
    fig.suptitle(f"Porównanie baz danych - Zbiór danych {size.capitalize()}\n({idx_title} | {scale_title})", 
                 fontsize=20, fontweight='bold', y=0.97)

    # Subplot mapping: (Axis, CRUD prefix, Title)
    crud_mappings = [
        (axes[0, 0], 'c', 'Create'),
        (axes[0, 1], 'r', 'Read'),
        (axes[1, 0], 'u', 'Update'),
        (axes[1, 1], 'd', 'Delete')
    ]

    for ax, prefix, title in crud_mappings:
        # Filter data by operation prefix (c, r, u, d)
        subset = df[df['scenario'].str.startswith(prefix)]
        
        if subset.empty:
            ax.set_title(f"{title} (No Data)")
            ax.axis('off')
            continue

        # Group by scenario and database, calculate mean
        agg_df = subset.groupby(['scenario', 'database'])['exec_time_seconds'].mean().unstack()
        
        # Ensure consistent column order even if some DBs are missing in a scenario
        existing_cols = [db for db in DB_ORDER if db in agg_df.columns]
        agg_df = agg_df[existing_cols]

        # Get corresponding colors
        colors = [DB_COLORS[db] for db in agg_df.columns]

        # Plot grouped bar chart
        agg_df.plot(kind='bar', ax=ax, color=colors, width=0.8, edgecolor='black', linewidth=0.5)

        # Formatting
        ax.set_title(title, fontsize=14, fontweight='bold')
        ax.set_ylabel('Średni czas wykonania [s]', fontsize=12)
        ax.set_xlabel('Scenariusz testowy', fontsize=12)
        ax.set_yscale(scale)
        ax.grid(axis='y', linestyle='--', alpha=0.7)
        ax.tick_params(axis='x', rotation=35, labelsize=10)
        
        # Adjust x-axis labels alignment for readability
        for tick in ax.get_xticklabels():
            tick.set_horizontalalignment('right')
            
        ax.legend(title='Baza danych', loc='best')

    # Adjust layout to fit titles and rotated labels
    plt.tight_layout(rect=[0, 0, 1, 0.95])
    
    # Save the figure
    filename = f"benchmarks/{size}_{index_state}_{scale}.png"
    filepath = os.path.join(CHARTS_DIR, filename)
    plt.savefig(filepath, dpi=150)
    plt.close(fig)
    print(f"Saved chart: {filepath}")


def plot_comparison(df_combined, index_state):
    """
    Generates comparison line plots (2x2 grids) showing how database performance 
    scales with dataset size. Outputs one figure for linear scale and one for log scale.
    """
    for scale in SCALES:
        fig, axes = plt.subplots(2, 2, figsize=(18, 14))
        
        idx_title = "Bez indeksów" if index_state == 'not_indexed' else "Z indeksami"
        scale_title = "Skala liniowa" if scale == 'linear' else "Skala logarytmiczna"
        fig.suptitle(f"Analiza trendu skalowalności baz danych\n({idx_title} | {scale_title})", 
                     fontsize=20, fontweight='bold', y=0.97)

        # Subplot mapping: (Axis, CRUD prefix, Title)
        crud_mappings = [
            (axes[0, 0], 'c', 'Create'),
            (axes[0, 1], 'r', 'Read'),
            (axes[1, 0], 'u', 'Update'),
            (axes[1, 1], 'd', 'Delete')
        ]

        for ax, prefix, title in crud_mappings:
            # Filter data by operation prefix
            subset = df_combined[df_combined['scenario'].str.startswith(prefix)]
            
            if subset.empty:
                ax.set_title(f"{title} (No Data)")
                ax.axis('off')
                continue

            # Group by dataset size and database, calculate overall mean
            agg_df = subset.groupby(['size', 'database'])['exec_time_seconds'].mean().unstack()
            
            # Ensure correct categorical order for X axis (small -> medium -> big)
            existing_sizes = [s for s in SIZES if s in agg_df.index]
            agg_df = agg_df.reindex(existing_sizes)

            # Ensure consistent database order
            existing_cols = [db for db in DB_ORDER if db in agg_df.columns]
            agg_df = agg_df[existing_cols]

            # Get corresponding colors
            colors = [DB_COLORS[db] for db in agg_df.columns]

            # Plot line chart
            agg_df.plot(kind='line', marker='o', markersize=8, linewidth=2.5, 
                        ax=ax, color=colors)

            # Formatting
            ax.set_title(title, fontsize=14, fontweight='bold')
            ax.set_ylabel('Średni czas wykonania [s]', fontsize=12)
            ax.set_xlabel('Rozmiar zbioru danych', fontsize=12)
            ax.set_yscale(scale)
            ax.grid(axis='both', linestyle='--', alpha=0.7)
            
            # Formatting the X ticks so 'small', 'medium', 'big' show up nicely
            ax.set_xticks(range(len(existing_sizes)))
            ax.set_xticklabels([s.capitalize() for s in existing_sizes], fontsize=11)
            
            ax.legend(title='Baza danych', loc='best')

        plt.tight_layout(rect=[0, 0, 1, 0.95])
        
        # Save the figure
        filename = f"scale_trends/{index_state}_{scale}.png"
        filepath = os.path.join(CHARTS_DIR, filename)
        plt.savefig(filepath, dpi=150)
        plt.close(fig)
        print(f"Saved trend chart: {filepath}")

def main():
    # Outer loop for index states to compare sizes within the same state
    for index_state in INDEX_STATES:
        state_dataframes = [] # Collect dataframes for the current index_state
        
        for size in SIZES:
            csv_filename = f"results_{index_state}_{size}.csv"
            csv_filepath = os.path.join(RESULTS_DIR, csv_filename)
            
            if not os.path.exists(csv_filepath):
                print(f"Warning: File {csv_filepath} not found. Skipping...")
                continue
                
            # Read data
            try:
                df = pd.read_csv(csv_filepath)
                # Inject 'size' column so the comparison function knows which is which
                df['size'] = size
                state_dataframes.append(df)
            except Exception as e:
                print(f"Error reading {csv_filepath}: {e}")
                continue
                
            # Generate individual bar charts for both normal (linear) and log scales
            for scale in SCALES:
                plot_benchmark_data(df, size, index_state, scale)
        
        # If we successfully loaded data for this index state, plot the comparison lines
        if state_dataframes:
            df_combined = pd.concat(state_dataframes, ignore_index=True)
            plot_comparison(df_combined, index_state)

if __name__ == "__main__":
    main()
