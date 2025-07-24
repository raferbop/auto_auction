import json
import pandas as pd
from datetime import datetime

def simple_convert_to_excel(json_file_path, output_excel_path=None):
    """
    Simple conversion of vehicle inventory JSON to Excel without formatting
    """
    
    try:
        # Read the JSON file
        with open(json_file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        
        # Extract the results array
        results = data.get('results', [])
        
        if not results:
            print("No results found in the JSON file.")
            return
        
        # Process the data
        processed_data = []
        
        for item in results:
            make = item.get('make', '')
            model = item.get('model', '')
            count = item.get('count', 0)
            
            # Combine make and model
            make_model = f"{make} {model}"
            
            processed_data.append({
                'Make Model': make_model,
                'Vehicle Count': count
            })
        
        # Create DataFrame
        df = pd.DataFrame(processed_data)
        
        # Sort by Vehicle Count in descending order
        df = df.sort_values('Vehicle Count', ascending=False)
        df = df.reset_index(drop=True)
        
        # Generate output filename if not provided
        if output_excel_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_excel_path = f"vehicle_inventory_{timestamp}.xlsx"
        
        # Save to Excel
        df.to_excel(output_excel_path, index=False, sheet_name='Inventory')
        
        print(f"Excel file created successfully: {output_excel_path}")
        print(f"Total records processed: {len(df)}")
        print(f"Total vehicles: {data.get('total_vehicles', 'N/A')}")
        
        # Display top 10 records
        print("\nTop 10 vehicle models by count:")
        print(df.head(10).to_string(index=False))
        
        # Create filtered version (count > 0)
        filtered_df = df[df['Vehicle Count'] > 0]
        filtered_output = output_excel_path.replace('.xlsx', '_filtered.xlsx')
        filtered_df.to_excel(filtered_output, index=False, sheet_name='Filtered_Inventory')
        
        print(f"\nFiltered Excel file created: {filtered_output}")
        print(f"Records with count > 0: {len(filtered_df)}")
        print(f"Total vehicle count in filtered data: {filtered_df['Vehicle Count'].sum()}")
        
        return output_excel_path, filtered_output
        
    except FileNotFoundError:
        print(f"Error: File '{json_file_path}' not found.")
        return None
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in '{json_file_path}'.")
        return None
    except Exception as e:
        print(f"Error: {str(e)}")
        return None

# Main execution
if __name__ == "__main__":
    # Update this with your actual JSON file path
    json_file_path = "inventory_results_optimized_20250721_131120.json"
    
    print("Simple Vehicle Inventory to Excel Converter")
    print("=" * 45)
    
    # Convert data
    result = simple_convert_to_excel(json_file_path)
    
    if result:
        print("\nConversion completed successfully!")
        print("Files created:")
        print(f"- {result[0]} (all data)")
        print(f"- {result[1]} (filtered data)")
    else:
        print("Conversion failed!")