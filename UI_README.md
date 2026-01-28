# 🍕 Decision-Ready Restaurant Reports - Demo UI

A simple web interface for uploading restaurant data and getting AI-powered business insights instantly.

## 🚀 Quick Start

1. **Open the interface:**
   ```
   http://localhost:8080
   ```

2. **Upload your ZIP file:**
   - Drag & drop a ZIP containing CSV files
   - Or click to browse and select

3. **Wait for AI processing:**
   - Takes 20-30 seconds for complete analysis
   - Progress bar shows real-time status

4. **View your results:**
   - **Business Analysis**: AI-generated markdown report with insights
   - **Interactive Dashboard**: Vega-Lite charts with restaurant metrics
   - **External Links**: Open full reports in new tabs

## 📁 Supported File Format

- **ZIP files** containing **CSV restaurant data**
- CSV files should include columns like:
  - Date, Sales, Location, Server, Orders, etc.
  - Any restaurant operational data

## 🎯 What You Get

### 📊 **AI-Powered Analysis**
- **Aggregated metrics** by location, date, server, etc.
- **Business insights** with actionable recommendations
- **Performance indicators** and trend analysis

### 📈 **Interactive Visualizations**
- **Bar charts** for sales by location
- **Line charts** for trends over time
- **Distribution charts** for order sources
- All charts are **interactive** with tooltips

### 📄 **Professional Reports**
- **Markdown business analysis** with structured insights
- **HTML dashboard** with embedded charts
- **S3-hosted files** for easy sharing

## 🔧 Technical Details

- **Frontend**: Bootstrap 5, JavaScript, Marked.js
- **Backend**: AWS Lambda (Container), Python
- **AI**: Google Gemini 2.0 Flash
- **Storage**: AWS S3 (public bucket)
- **Charts**: Vega-Lite specifications

## 🌐 Demo Features

- ✅ **Drag & Drop Upload**
- ✅ **Real-time Progress**
- ✅ **Error Handling**
- ✅ **Responsive Design**
- ✅ **Split View Results**
- ✅ **External Link Access**
- ✅ **Mobile Friendly**

## 🎪 Perfect for Hackathon Demo!

This interface showcases the complete end-to-end workflow:
1. **Data Upload** → 2. **AI Processing** → 3. **Instant Insights** → 4. **Business Decisions**

Transform raw restaurant data into actionable business intelligence in under 30 seconds! 🚀
