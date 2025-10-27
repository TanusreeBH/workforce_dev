"""
Download Total US Labor Force Data and Calculate Native-Born
Native-Born = Total US - Foreign-Born
"""

import pandas as pd
import os

print("\n" + "="*70)
print("CALCULATE NATIVE-BORN DATA")
print("="*70)

# ============================================================================
# STEP 1: Download Total US Labor Force Data from FRED
# ============================================================================

print("\n📥 Downloading Total US Labor Force Data from FRED...")

total_us_series = {
    'Total_US_Labor_Force': 'CLF16OV',
    'Total_US_Employment': 'CE16OV',
    'Total_US_LF_Participation': 'CIVPART',
}

total_data = {}

for name, series_id in total_us_series.items():
    print(f"\n  → {name}...")
    print(f"     Series ID: {series_id}")
    
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
    
    try:
        df = pd.read_csv(url)
        df.columns = ['Date', name]
        df['Date'] = pd.to_datetime(df['Date'])
        df = df[df['Date'] >= '2015-01-01']
        
        total_data[name] = df
        
        # Save
        filename = f'labor_market_data/fred_{series_id}_{name}.csv'
        df.to_csv(filename, index=False)
        
        print(f"     ✓ Downloaded {len(df)} records")
        print(f"     ✓ Saved to: {filename}")
        
    except Exception as e:
        print(f"     ✗ Failed: {e}")

# ============================================================================
# STEP 2: Load Foreign-Born Data
# ============================================================================

print("\n\n📊 Loading Foreign-Born Data...")

try:
    fb_lf = pd.read_csv('labor_market_data/fred_LNU01073395_Foreign_Born_Labor_Force.csv')
    fb_emp = pd.read_csv('labor_market_data/fred_LNU02073395_Foreign_Born_Employment.csv')
    fb_lfp = pd.read_csv('labor_market_data/fred_LNU01373395_Foreign_Born_LF_Participation.csv')
    
    print("  ✓ Loaded Foreign-Born Labor Force")
    print("  ✓ Loaded Foreign-Born Employment")
    print("  ✓ Loaded Foreign-Born LF Participation")
    
except Exception as e:
    print(f"  ✗ Error loading foreign-born data: {e}")
    print("  → Please run the download script first!")
    exit(1)

# ============================================================================
# STEP 3: Calculate Native-Born = Total - Foreign-Born
# ============================================================================

print("\n\n🧮 Calculating Native-Born Data...")

try:
    # Merge total US and foreign-born data
    merged = total_data['Total_US_Labor_Force'].copy()
    merged = merged.merge(fb_lf, on='Date', how='inner')
    merged = merged.merge(total_data['Total_US_Employment'], on='Date', how='inner')
    merged = merged.merge(fb_emp, on='Date', how='inner')
    merged = merged.merge(total_data['Total_US_LF_Participation'], on='Date', how='inner')
    merged = merged.merge(fb_lfp, on='Date', how='inner')
    
    # Calculate Native-Born (thousands)
    merged['Native_Born_Labor_Force'] = (
        merged['Total_US_Labor_Force'] - merged['Foreign_Born_Labor_Force']
    )
    merged['Native_Born_Employment'] = (
        merged['Total_US_Employment'] - merged['Foreign_Born_Employment']
    )
    
    # Calculate Native-Born LF Participation Rate
    # This is approximate since we don't have exact population data
    # But we can estimate from the ratio
    total_pop_in_lf = merged['Total_US_Labor_Force'] / (merged['Total_US_LF_Participation'] / 100)
    fb_pop_in_lf = merged['Foreign_Born_Labor_Force'] / (merged['Foreign_Born_LF_Participation'] / 100)
    nb_pop = total_pop_in_lf - fb_pop_in_lf
    merged['Native_Born_LF_Participation'] = (
        merged['Native_Born_Labor_Force'] / nb_pop * 100
    )
    
    # Calculate Employment-to-Population Ratios
    merged['Foreign_Born_Emp_Pop_Ratio'] = (
        merged['Foreign_Born_Employment'] / fb_pop_in_lf * 100
    )
    merged['Native_Born_Emp_Pop_Ratio'] = (
        merged['Native_Born_Employment'] / nb_pop * 100
    )
    
    print("  ✓ Calculated Native-Born Labor Force")
    print("  ✓ Calculated Native-Born Employment")
    print("  ✓ Calculated Native-Born LF Participation")
    print("  ✓ Calculated Employment-Population Ratios")
    
    # Save complete dataset
    output_file = 'labor_market_data/complete_labor_force_data.csv'
    merged.to_csv(output_file, index=False)
    
    print(f"\n  💾 Saved complete dataset: {output_file}")
    print(f"     Shape: {merged.shape[0]} rows × {merged.shape[1]} columns")
    
except Exception as e:
    print(f"  ✗ Error calculating: {e}")
    import traceback
    traceback.print_exc()

# ============================================================================
# STEP 4: Create Comparison Summary
# ============================================================================

print("\n\n📊 Creating Comparison Summary...")

try:
    # Select key columns
    comparison = merged[[
        'Date',
        'Foreign_Born_Labor_Force',
        'Native_Born_Labor_Force',
        'Total_US_Labor_Force',
        'Foreign_Born_Employment',
        'Native_Born_Employment',
        'Total_US_Employment',
        'Foreign_Born_LF_Participation',
        'Native_Born_LF_Participation',
        'Foreign_Born_Emp_Pop_Ratio',
        'Native_Born_Emp_Pop_Ratio'
    ]].copy()
    
    # Calculate shares
    comparison['Foreign_Born_Share_%'] = (
        comparison['Foreign_Born_Labor_Force'] / 
        comparison['Total_US_Labor_Force'] * 100
    )
    
    # Save
    output_file = 'labor_market_data/foreign_vs_native_comparison.csv'
    comparison.to_csv(output_file, index=False)
    
    print(f"  ✓ Saved comparison: {output_file}")
    
    # Show recent data
    print(f"\n  📈 Latest Data (Most Recent Month):")
    latest = comparison.iloc[-1]
    print(f"     Date: {latest['Date']}")
    print(f"\n     Foreign-Born:")
    print(f"       Labor Force: {latest['Foreign_Born_Labor_Force']:>10,.0f} thousand")
    print(f"       Employment:  {latest['Foreign_Born_Employment']:>10,.0f} thousand")
    print(f"       LF Particip: {latest['Foreign_Born_LF_Participation']:>10.1f}%")
    print(f"       Emp-Pop Ratio: {latest['Foreign_Born_Emp_Pop_Ratio']:>8.1f}%")
    print(f"\n     Native-Born:")
    print(f"       Labor Force: {latest['Native_Born_Labor_Force']:>10,.0f} thousand")
    print(f"       Employment:  {latest['Native_Born_Employment']:>10,.0f} thousand")
    print(f"       LF Particip: {latest['Native_Born_LF_Participation']:>10.1f}%")
    print(f"       Emp-Pop Ratio: {latest['Native_Born_Emp_Pop_Ratio']:>8.1f}%")
    print(f"\n     Foreign-Born Share: {latest['Foreign_Born_Share_%']:.1f}%")
    
except Exception as e:
    print(f"  ✗ Error creating comparison: {e}")

# ============================================================================
# STEP 5: Create Annual Averages
# ============================================================================

print("\n\n📅 Creating Annual Averages...")

try:
    comparison['Year'] = pd.to_datetime(comparison['Date']).dt.year
    
    annual = comparison.groupby('Year').mean(numeric_only=True).reset_index()
    annual = annual[(annual['Year'] >= 2015) & (annual['Year'] <= 2024)]
    
    output_file = 'labor_market_data/annual_foreign_vs_native.csv'
    annual.to_csv(output_file, index=False)
    
    print(f"  ✓ Saved annual averages: {output_file}")
    print(f"\n  📊 Annual Summary (2015 vs 2024):")
    
    if len(annual) > 0:
        year_2015 = annual[annual['Year'] == 2015].iloc[0]
        year_2024 = annual[annual['Year'] == 2024].iloc[0]
        
        print(f"\n  2015:")
        print(f"    FB Labor Force: {year_2015['Foreign_Born_Labor_Force']:>8,.0f}k  |  NB: {year_2015['Native_Born_Labor_Force']:>9,.0f}k")
        print(f"    FB Share: {year_2015['Foreign_Born_Share_%']:>5.1f}%")
        
        print(f"\n  2024:")
        print(f"    FB Labor Force: {year_2024['Foreign_Born_Labor_Force']:>8,.0f}k  |  NB: {year_2024['Native_Born_Labor_Force']:>9,.0f}k")
        print(f"    FB Share: {year_2024['Foreign_Born_Share_%']:>5.1f}%")
        
        print(f"\n  Growth (2015-2024):")
        fb_growth = ((year_2024['Foreign_Born_Labor_Force'] / year_2015['Foreign_Born_Labor_Force']) - 1) * 100
        nb_growth = ((year_2024['Native_Born_Labor_Force'] / year_2015['Native_Born_Labor_Force']) - 1) * 100
        print(f"    Foreign-Born: +{fb_growth:.1f}%")
        print(f"    Native-Born:  +{nb_growth:.1f}%")
    
except Exception as e:
    print(f"  ✗ Error creating annual: {e}")

# ============================================================================
# FINAL SUMMARY
# ============================================================================

print("\n\n" + "="*70)
print("✅ COMPLETE DATA CALCULATED!")
print("="*70)

print("\n📁 New Files Created:")
print("  1. complete_labor_force_data.csv")
print("  2. foreign_vs_native_comparison.csv")
print("  3. annual_foreign_vs_native.csv")
print("  4. fred_CLF16OV_Total_US_Labor_Force.csv")
print("  5. fred_CE16OV_Total_US_Employment.csv")
print("  6. fred_CIVPART_Total_US_LF_Participation.csv")

print("\n" + "="*70)
print("📊 YOU NOW HAVE COMPLETE DATA!")
print("="*70)
print("""
✓ Foreign-Born Labor Force, Employment, Participation
✓ Native-Born Labor Force, Employment, Participation (calculated)
✓ Total US Labor Force data
✓ Monthly data (2015-2025)
✓ Annual averages (2015-2024)
✓ Foreign-Born share trends
✓ Employment-to-population ratios

Still needed:
• BLS Industry Distribution (manual download)
• BLS Occupation Distribution (manual download)
""")

print("="*70)
print("🎯 NEXT: Download BLS tables manually")
print("="*70)
print("""
1. Industry Distribution:
   https://www.bls.gov/news.release/forbrn.t03.htm

2. Occupation Distribution:
   https://www.bls.gov/news.release/forbrn.t02.htm

Copy tables to Excel, save as CSV in labor_market_data/ folder
""")

print("="*70)