# 📊 Power BI DAX Measures Library
**Project:** Logistics Operations & Analytics Platform  
**Target Schema:** Google BigQuery (`logistics_gold` & `logistics_audit`)

---

## 1. DAX Architecture & Organization

To maintain a clean and scalable Power BI model, all measures are stored inside a dedicated disconnected table named **`_Measures`** and organized into distinct **Display Folders**:

```
📁 _Measures
  ├── 📁 01 Financial & Commercial
  ├── 📁 02 Fleet Operations & Transit
  ├── 📁 03 Service Level & Delivery Quality
  ├── 📁 04 Safety & Driver Compliance
  ├── 📁 05 Data Quality & Pipeline Governance
  └── 📁 06 Time Intelligence (YoY, MoM)
```

> [!TIP]
> **Creation of `_Measures` Table in Power BI:**
> In Power BI Desktop: Click **Enter Data** $\rightarrow$ Name table `_Measures` $\rightarrow$ Click **Load**. Create your first measure, then delete the dummy column `Column1`.

---

## 2. Display Folder: 01 Financial & Commercial

### `[Total Revenue]`
* **Description:** Sum of all revenue streams (freight base rate, fuel surcharge, and accessorial charges).
* **Format:** Currency (`$#,##0.00`)
```dax
Total Revenue = 
SUM(fact_trips[total_revenue])
```

### `[Freight Revenue]`
* **Description:** Base linehaul transportation revenue earned.
* **Format:** Currency (`$#,##0.00`)
```dax
Freight Revenue = 
SUM(fact_trips[freight_revenue])
```

### `[Fuel Surcharge Revenue]`
* **Description:** Fuel surcharge billed to customers to hedge diesel price fluctuations.
* **Format:** Currency (`$#,##0.00`)
```dax
Fuel Surcharge Revenue = 
SUM(fact_trips[fuel_surcharge])
```

### `[Accessorial Revenue]`
* **Description:** Ancillary charges (detention, layover, driver assist, re-delivery).
* **Format:** Currency (`$#,##0.00`)
```dax
Accessorial Revenue = 
SUM(fact_trips[accessorial_charges])
```

### `[Total Fuel Expense]`
* **Description:** Total fuel expense invoiced at pumps across all trips.
* **Format:** Currency (`$#,##0.00`)
```dax
Total Fuel Expense = 
SUM(fact_fuel_purchases[total_cost])
```

### `[Total Maintenance Expense]`
* **Description:** Total parts and labor servicing costs.
* **Format:** Currency (`$#,##0.00`)
```dax
Total Maintenance Expense = 
SUM(fact_maintenance[total_cost])
```

### `[Total Operating Cost]`
* **Description:** Combined direct operating expenses (Fuel + Fleet Maintenance).
* **Format:** Currency (`$#,##0.00`)
```dax
Total Operating Cost = 
[Total Fuel Expense] + [Total Maintenance Expense]
```

### `[Operating Margin $]`
* **Description:** Net operating profit contribution before corporate overhead.
* **Format:** Currency (`$#,##0.00`)
```dax
Operating Margin $ = 
[Total Revenue] - [Total Operating Cost]
```

### `[Operating Margin %]`
* **Description:** Percentage of gross revenue retained after direct operating costs.
* **Format:** Percentage (`0.0%`)
```dax
Operating Margin % = 
DIVIDE([Operating Margin $], [Total Revenue], 0)
```

### `[Revenue Per Mile (RPM)]`
* **Description:** Primary trucking profitability metric (gross revenue per loaded/dispatch mile).
* **Format:** Currency (`$#,##0.00`)
```dax
Revenue Per Mile (RPM) = 
DIVIDE([Total Revenue], [Total Miles Driven], 0)
```

### `[Cost Per Mile (CPM)]`
* **Description:** Direct operating expense per dispatched fleet mile.
* **Format:** Currency (`$#,##0.00`)
```dax
Cost Per Mile (CPM) = 
DIVIDE([Total Operating Cost], [Total Miles Driven], 0)
```

### `[Net Margin Per Mile]`
* **Description:** Operating profit earned per fleet mile.
* **Format:** Currency (`$#,##0.00`)
```dax
Net Margin Per Mile = 
[Revenue Per Mile (RPM)] - [Cost Per Mile (CPM)]
```

### `[Average Revenue Per Trip]`
* **Description:** Average billing ticket size per trip dispatch.
* **Format:** Currency (`$#,##0.00`)
```dax
Average Revenue Per Trip = 
DIVIDE([Total Revenue], [Total Trips], 0)
```

---

## 3. Display Folder: 02 Fleet Operations & Transit

### `[Total Trips]`
* **Description:** Total number of completed dispatches.
* **Format:** Whole Number (`#,##0`)
```dax
Total Trips = 
COUNTROWS(fact_trips)
```

### `[Total Miles Driven]`
* **Description:** Cumulative odometer miles driven across all loads.
* **Format:** Whole Number (`#,##0`)
```dax
Total Miles Driven = 
SUM(fact_trips[actual_distance_miles])
```

### `[Average Trip Distance (Miles)]`
* **Description:** Average haul distance per trip.
* **Format:** Decimal (`#,##0.0`)
```dax
Average Trip Distance (Miles) = 
AVERAGE(fact_trips[actual_distance_miles])
```

### `[Average Trip Duration (Hours)]`
* **Description:** Average transit hours per dispatch.
* **Format:** Decimal (`#,##0.0`)
```dax
Average Trip Duration (Hours) = 
AVERAGE(fact_trips[actual_duration_hours])
```

### `[Average Transit Speed (MPH)]`
* **Description:** Average highway travel speed across all trips.
* **Format:** Decimal (`#,##0.0`)
```dax
Average Transit Speed (MPH) = 
DIVIDE([Total Miles Driven], SUM(fact_trips[actual_duration_hours]), 0)
```

### `[Fleet Fuel Economy (MPG)]`
* **Description:** Average miles driven per gallon of diesel consumed.
* **Format:** Decimal (`0.00`)
```dax
Fleet Fuel Economy (MPG) = 
DIVIDE([Total Miles Driven], SUM(fact_trips[fuel_gallons_used]), 0)
```

### `[Total Fuel Gallons Consumed]`
* **Description:** Total diesel gallons pumped into fleet vehicles.
* **Format:** Decimal (`#,##0.0`)
```dax
Total Fuel Gallons Consumed = 
SUM(fact_fuel_purchases[gallons])
```

### `[Average Fuel Price Per Gallon]`
* **Description:** Average diesel purchase price.
* **Format:** Currency (`$#,##0.000`)
```dax
Average Fuel Price Per Gallon = 
DIVIDE([Total Fuel Expense], [Total Fuel Gallons Consumed], 0)
```

### `[Active Truck Count]`
* **Description:** Distinct active trucks registered in fleet.
* **Format:** Whole Number (`#,##0`)
```dax
Active Truck Count = 
DISTINCTCOUNT(fact_trips[truck_id])
```

### `[Fleet Utilization %]`
* **Description:** Average operational utilization rate across all active trucks.
* **Format:** Percentage (`0.0%`)
```dax
Fleet Utilization % = 
AVERAGE(agg_truck_utilization_performance[utilization_rate])
```

### `[Total Maintenance Downtime (Hours)]`
* **Description:** Total hours vehicles were out of service for servicing/repairs.
* **Format:** Whole Number (`#,##0`)
```dax
Total Maintenance Downtime (Hours) = 
SUM(fact_maintenance[downtime_hours])
```

### `[Total Maintenance Events]`
* **Description:** Total number of work orders and maintenance shop visits.
* **Format:** Whole Number (`#,##0`)
```dax
Total Maintenance Events = 
COUNTROWS(fact_maintenance)
```

---

## 4. Display Folder: 03 Service Level & Delivery Quality

### `[Total Delivery Events]`
* **Description:** Total milestone events recorded (Pickups and Deliveries).
* **Format:** Whole Number (`#,##0`)
```dax
Total Delivery Events = 
COUNTROWS(fact_delivery_events)
```

### `[On-Time Deliveries]`
* **Description:** Count of deliveries meeting or beating scheduled appointment time.
* **Format:** Whole Number (`#,##0`)
```dax
On-Time Deliveries = 
CALCULATE(
    COUNTROWS(fact_delivery_events),
    fact_delivery_events[on_time_flag] = TRUE()
)
```

### `[Delayed Deliveries]`
* **Description:** Count of shipments arriving past schedule.
* **Format:** Whole Number (`#,##0`)
```dax
Delayed Deliveries = 
CALCULATE(
    COUNTROWS(fact_delivery_events),
    fact_delivery_events[on_time_flag] = FALSE()
)
```

### `[On-Time Delivery Rate (OTD %)]`
* **Description:** Core SLA metric: percentage of shipments completed on time.
* **Format:** Percentage (`0.0%`)
```dax
On-Time Delivery Rate (OTD %) = 
DIVIDE([On-Time Deliveries], [Total Delivery Events], 0)
```

### `[Total Detention Minutes]`
* **Description:** Driver waiting/dwell minutes accumulated at facility loading docks.
* **Format:** Whole Number (`#,##0`)
```dax
Total Detention Minutes = 
SUM(fact_delivery_events[detention_minutes])
```

### `[Average Detention Per Stop (Mins)]`
* **Description:** Mean dwell time experienced per facility visit.
* **Format:** Decimal (`#,##0.0`)
```dax
Average Detention Per Stop (Mins) = 
AVERAGE(fact_delivery_events[detention_minutes])
```

### `[Severe Detention Incidents (> 2 Hrs)]`
* **Description:** Count of excessive detention stops exceeding industry standard 120-minute threshold.
* **Format:** Whole Number (`#,##0`)
```dax
Severe Detention Incidents (> 2 Hrs) = 
CALCULATE(
    COUNTROWS(fact_delivery_events),
    fact_delivery_events[detention_minutes] > 120
)
```

---

## 5. Display Folder: 04 Safety & Driver Compliance

### `[Active Driver Count]`
* **Description:** Number of unique drivers actively hauling loads.
* **Format:** Whole Number (`#,##0`)
```dax
Active Driver Count = 
DISTINCTCOUNT(fact_trips[driver_id])
```

### `[Total Safety Incidents]`
* **Description:** Total safety incidents logged across all trips.
* **Format:** Whole Number (`#,##0`)
```dax
Total Safety Incidents = 
COUNTROWS(fact_safety_incidents)
```

### `[Preventable Incidents]`
* **Description:** Incidents deemed driver-preventable during safety audit.
* **Format:** Whole Number (`#,##0`)
```dax
Preventable Incidents = 
CALCULATE(
    COUNTROWS(fact_safety_incidents),
    fact_safety_incidents[preventable_flag] = TRUE()
)
```

### `[Preventable Incident Rate %]`
* **Description:** Proportion of incidents that could have been avoided with defensive driving.
* **Format:** Percentage (`0.0%`)
```dax
Preventable Incident Rate % = 
DIVIDE([Preventable Incidents], [Total Safety Incidents], 0)
```

### `[At-Fault Incidents]`
* **Description:** Incidents where company fleet vehicle was determined at-fault.
* **Format:** Whole Number (`#,##0`)
```dax
At-Fault Incidents = 
CALCULATE(
    COUNTROWS(fact_safety_incidents),
    fact_safety_incidents[at_fault_flag] = TRUE()
)
```

### `[Safety Incidents Per 100k Miles]`
* **Description:** DOT standard safety frequency index.
* **Format:** Decimal (`0.00`)
```dax
Safety Incidents Per 100k Miles = 
DIVIDE([Total Safety Incidents] * 100000, [Total Miles Driven], 0)
```

### `[Total Damage & Claim Cost]`
* **Description:** Cumulative cost of vehicle damage, cargo loss, and liability claims.
* **Format:** Currency (`$#,##0.00`)
```dax
Total Damage & Claim Cost = 
SUM(fact_safety_incidents[vehicle_damage_cost]) +
SUM(fact_safety_incidents[cargo_damage_cost]) +
SUM(fact_safety_incidents[claim_amount])
```

---

## 6. Display Folder: 05 Data Quality & Pipeline Governance

### `[Total Records Ingested]`
* **Description:** Total incoming raw records processed through Bronze/Silver stages.
* **Format:** Whole Number (`#,##0`)
```dax
Total Records Ingested = 
SUM(pipeline_audit[records_in])
```

### `[Total Records Validated (Silver)]`
* **Description:** Records successfully passing all schema and business validation rules.
* **Format:** Whole Number (`#,##0`)
```dax
Total Records Validated (Silver) = 
SUM(pipeline_audit[records_out])
```

### `[Total Records Quarantined]`
* **Description:** Invalid records routed to quarantine due to rule violations.
* **Format:** Whole Number (`#,##0`)
```dax
Total Records Quarantined = 
SUM(pipeline_audit[records_quarantined])
```

### `[Rejection Rate %]`
* **Description:** Percentage of ingested data failing quality validation checks.
* **Format:** Percentage (`0.00%`)
```dax
Rejection Rate % = 
DIVIDE([Total Records Quarantined], [Total Records Ingested], 0)
```

### `[Overall Data Quality Score %]`
* **Description:** Average data quality compliance score logged across all evaluated tables.
* **Format:** Percentage (`0.00%`)
```dax
Overall Data Quality Score % = 
AVERAGE(data_quality_metrics[data_quality_score_pct])
```

### `[Total Pipeline Duration (Minutes)]`
* **Description:** Cumulative elapsed execution time across Bronze, Silver, and Gold stages.
* **Format:** Decimal (`#,##0.0`)
```dax
Total Pipeline Duration (Minutes) = 
DIVIDE(SUM(pipeline_audit[duration_seconds]), 60, 0)
```

---

## 7. Display Folder: 06 Time Intelligence (YoY & MoM)

### `[Revenue YTD]`
* **Description:** Year-to-date cumulative revenue.
* **Format:** Currency (`$#,##0.00`)
```dax
Revenue YTD = 
TOTALYTD([Total Revenue], dim_date[full_date])
```

### `[Revenue Prior Year (YoY)]`
* **Description:** Revenue earned in the identical period of the previous calendar year.
* **Format:** Currency (`$#,##0.00`)
```dax
Revenue Prior Year (YoY) = 
CALCULATE(
    [Total Revenue],
    SAMEPERIODLASTYEAR(dim_date[full_date])
)
```

### `[Revenue YoY Growth %]`
* **Description:** Percentage variance in revenue compared to prior year.
* **Format:** Percentage (`+0.0%;-0.0%;0.0%`)
```dax
Revenue YoY Growth % = 
DIVIDE([Total Revenue] - [Revenue Prior Year (YoY)], [Revenue Prior Year (YoY)], 0)
```

### `[Revenue Previous Month (MoM)]`
* **Description:** Revenue earned in the immediately preceding month.
* **Format:** Currency (`$#,##0.00`)
```dax
Revenue Previous Month (MoM) = 
CALCULATE(
    [Total Revenue],
    PREVIOUSMONTH(dim_date[full_date])
)
```

### `[Revenue MoM Growth %]`
* **Description:** Percentage variance in revenue compared to previous month.
* **Format:** Percentage (`+0.0%;-0.0%;0.0%`)
```dax
Revenue MoM Growth % = 
DIVIDE([Total Revenue] - [Revenue Previous Month (MoM)], [Revenue Previous Month (MoM)], 0)
```

