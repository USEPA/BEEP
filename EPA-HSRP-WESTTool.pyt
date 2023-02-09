# -*- coding: utf-8 -*-

import arcpy;
import requests,json,csv,zipfile;
import openpyxl;
from openpyxl import Workbook;
from openpyxl.styles import Font,Alignment;
from openpyxl.utils import get_column_letter;

try:
   from arcpy.sa import *;
except:
   None;

# =========================================================================================
# Plume Tool; Waste Estimation Support Tool (WEST) US EPA
# Created by: Timothy Boe, Paul Lemieux
# Last Modified: 05.11.15; WEST v3
# Description: 
#  1) Loads, merges (if necessary), and massages plume shapefile; 
#  2) Establishes the area and distribution of the plume within the census tracts; 
#  3) Generates a KML files of the census tracts; 
#  4) Capture imagery clipped to the plume
# Input:  Shapefile
# Output: Census.txt, Output.csv, Plumearea.csv, doc.kml, Zone1.bmp, Zone2.bmp, Zone3.bmp
# =========================================================================================

g_prj      = "CURRENT";
g_srid     = 4326;
g_mapname  = "WESTTool";
g_imagemap = "ImageryExtract";

###############################################################################
class Toolbox(object):

   def __init__(self):
      self.label = "EPA-HSRP-WESTTool";
      self.alias = "EPA-HSRP-WESTTool";

      self.tools = [
          FetchCensusTracts
         ,AddUSAStructures
         ,SetupWorking
         ,ProcessPlumeScenario
      ];
 
############################################################################### 
class FetchCensusTracts(object):

   def __init__(self):
      self.label = "A1 Fetch Census Tracts";
      self.description = "";
      self.canRunInBackground = False;

   def getParameterInfo(self):
   
      ##---------------------------------------------------------------------##
      param0 = arcpy.Parameter(
          displayName   = "Host"
         ,name          = "Host"
         ,datatype      = "GPString"
         ,parameterType = "Required"
         ,direction     = "Input"
         ,enabled       = True
      );
      param0.value = "tigerweb.geo.census.gov";
      
      ##---------------------------------------------------------------------##
      param1 = arcpy.Parameter(
          displayName   = "Service Path"
         ,name          = "ServicePath"
         ,datatype      = "GPString"
         ,parameterType = "Required"
         ,direction     = "Input"
         ,enabled       = True
      );
      param1.value = "/arcgis/rest/services/TIGERweb/tigerWMS_Census2010/MapServer/14";
      
      ##---------------------------------------------------------------------##
      param2 = arcpy.Parameter(
          displayName   = "Override Max Request"
         ,name          = "OverrideMaxRequest"
         ,datatype      = "GPLong"
         ,parameterType = "Optional"
         ,direction     = "Input"
         ,enabled       = True
      );
      param2.value = 1000;
      
      return [
          param0
         ,param1
         ,param2
      ];
      
   def isLicensed(self):
      return True;

   def updateParameters(self, parameters):
      return;

   def updateMessages(self, parameters):
      return;

   def execute(self, parameters, messages):      
      aprx = arcpy.mp.ArcGISProject(g_prj);
      
      host         = parameters[0].valueAsText;
      service_path = parameters[1].valueAsText;
      override_max = parameters[2].value;
      target       = aprx.defaultGeodatabase + os.sep + 'tracts2010';
      
      arcpy.AddMessage("(Re)building Tracts dataset.");
      if arcpy.Exists(target):
         arcpy.Delete_management(target);
         
      arcpy.CreateFeatureclass_management(
          out_path          = os.path.dirname(target)
         ,out_name          = os.path.basename(target)
         ,geometry_type     = "POLYGON"
         ,has_m             = "DISABLED"
         ,has_z             = "DISABLED"
         ,spatial_reference = arcpy.SpatialReference(g_srid)
         ,config_keyword    = None
      );
      
      arcpy.management.AddFields(
          target
         ,[
             ['MTFCC'         ,'TEXT'  ,'MTFCC'     ,5   ,None,'']
            ,['OID'           ,'TEXT'  ,'OID'       ,22  ,None,'']
            ,['GEOID'         ,'TEXT'  ,'GEOID'     ,11  ,None,'']
            ,['STATE'         ,'TEXT'  ,'STATE'     ,2   ,None,'']
            ,['COUNTY'        ,'TEXT'  ,'COUNTY'    ,3   ,None,'']
            ,['TRACT'         ,'TEXT'  ,'TRACT'     ,6   ,None,'']
            ,['BASENAME'      ,'TEXT'  ,'BASENAME'  ,100 ,None,'']
            ,['NAME'          ,'TEXT'  ,'NAME'      ,100 ,None,'']
            ,['LSADC'         ,'TEXT'  ,'LSADC'     ,2   ,None,'']
            ,['FUNCSTAT'      ,'TEXT'  ,'FUNCSTAT'  ,1   ,None,'']
            ,['AREALAND'      ,'DOUBLE','AREALAND'  ,None,None,'']            
            ,['AREAWATER'     ,'DOUBLE','AREAWATER' ,None,None,'']
            ,['UR'            ,'TEXT'  ,'UR'        ,1   ,None,'']            
            ,['CENTLAT'       ,'TEXT'  ,'CENTLAT'   ,11  ,None,'']           
            ,['CENTLON'       ,'TEXT'  ,'CENTLON'   ,12  ,None,'']
            ,['INTPTLAT'      ,'TEXT'  ,'INTPTLAT'  ,11  ,None,'']
            ,['INTPTLON'      ,'TEXT'  ,'INTPTLON'  ,12  ,None,'']
            ,['HU100'         ,'DOUBLE','HU100'     ,None,None,'']            
            ,['POP100'        ,'DOUBLE','POP100'    ,None,None,'']
         ]
      );
      
      arcpy.AddMessage("Configuring against " + str(host) + str(service_path) + ".");
      if override_max is None:
         headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"};
         parms   = {"f": "json"};
         r = requests.get('https://' + host + service_path,params=parms);
         r_json = r.json();
         extraction_amount = r_json['maxRecordCount'];
         
         if not 'currentVersion' in r_json:
            raise ValueError("Error, unable to query https://" + host + service_path);
      
      else:
         extraction_amount = override_max;
         
      arcpy.AddMessage("Extraction Amount: " + str(extraction_amount));
      
      where = "1=1";
      
      parms = {"where": where,"returnCountOnly": "true","f": "json"};
      r = requests.get('https://' + host + service_path + '/query',params=parms);
      r_json = r.json();
      if not 'count' in r_json:
         raise ValueError("Error, unable to query https://" + host + service_path);
      total_records = r_json['count'];
      arcpy.AddMessage("Total Records to fetch: " + str(total_records));
      
      result_offset = 0;
      initial_hit = True;
      while result_offset <= total_records:
       
         arcpy.AddMessage("Downloading from offset " + str(result_offset));
         
         parms = {
             "where": where
            ,"outFields": "*"
            ,"resultOffset": result_offset
            ,"resultRecordCount": extraction_amount
            ,"returnGeometry": "true"
            ,"outSR": g_srid
            ,"f": "json"
         };

         r = requests.get('https://' + host + service_path + '/query',params=parms);
         json_data = r.json();
         ef = arcpy.AsShape(json_data,True)
         
         if initial_hit:
            arcpy.management.CopyFeatures(ef,target)
            initial_hit = False;
         else:
            arcpy.Append_management(ef,target,"NO_TEST");
            
         result_offset += extraction_amount;
      
      arcpy.AddMessage("Adding WEST area field.");
      
      arcpy.management.AddField(
          in_table     = 'tracts2010'
         ,field_name   = 'TRACTAREASQM'
         ,field_type   = 'DOUBLE'
         ,field_alias  = 'Tract Area (SqM)'
      );
      
      arcpy.CalculateField_management(
          in_table        = 'tracts2010'
         ,field           = 'TRACTAREASQM'
         ,expression      = '!Shape!.getArea("GEODESIC","SQUAREMETERS")'
         ,expression_type = "PYTHON"
      );
      
      arcpy.AddMessage("Adding indexes.");
      
      arcpy.AddIndex_management(
          in_table   = 'tracts2010'
         ,fields     = 'TRACT'
         ,index_name = 'TRACT_IDX'
      );
      
      arcpy.AddIndex_management(
          in_table   = 'tracts2010'
         ,fields     = 'GEOID'
         ,index_name = 'GEOID_IDX'
      );
      
      arcpy.AddMessage("Tracts download complete.");
      
      return;

   def postExecute(self, parameters):
      return; 
      
############################################################################### 
class AddUSAStructures(object):

   def __init__(self):
      self.label = "A2 Add USA Structures";
      self.description = "";
      self.canRunInBackground = False;

   def getParameterInfo(self):
   
      ##---------------------------------------------------------------------##
      param0 = arcpy.Parameter(
          displayName   = "Living Atlas USA Structures"
         ,name          = "LivingAtlasUSAStructures"
         ,datatype      = "GPString"
         ,parameterType = "Required"
         ,direction     = "Input"
         ,enabled       = True
      );
      param0.value = "0ec8512ad21e4bb987d7e848d14e7e24";
      
      return [
          param0
      ];
      
   def isLicensed(self):
      return True;

   def updateParameters(self, parameters):
      return;

   def updateMessages(self, parameters):
      return;

   def execute(self, parameters, messages):      
      aprx = arcpy.mp.ArcGISProject(g_prj);
      map = aprx.listMaps(g_mapname)[0];
      
      for lyr in map.listLayers():
         if lyr.supports("name"):
            if lyr.name == "USA_Structures":
               map.removeLayer(lyr);
                  
      sourceURI = parameters[0].valueAsText;
      
      lyrx_usas = tempLyrx(
          in_layerfile = aprx.homeFolder + os.sep + 'USA_Structures.lyrx'
         ,sourceURI    = sourceURI
         ,name         = 'USA_Structures'
         ,aprx         = aprx
      );
      lyr_usas = arcpy.mp.LayerFile(lyrx_usas);
      map.addLayer(lyr_usas,"AUTO_ARRANGE");
      
      return;

   def postExecute(self, parameters):
      return; 
 
############################################################################### 
class SetupWorking(object):

   def __init__(self):
      self.label = "A3 Setup Working";
      self.description = "";
      self.canRunInBackground = False;

   def getParameterInfo(self):
      return [];
      
   def isLicensed(self):
      return True;

   def updateParameters(self, parameters):
      return;

   def updateMessages(self, parameters):
      return;

   def execute(self, parameters, messages):      
      aprx = arcpy.mp.ArcGISProject(g_prj);
      
      target = aprx.defaultGeodatabase + os.sep + 'plumeTemplate';
      
      if arcpy.Exists(target):
         arcpy.Delete_management(target);
         
      arcpy.CreateFeatureclass_management(
          out_path          = os.path.dirname(target)
         ,out_name          = os.path.basename(target)
         ,geometry_type     = "POLYGON"
         ,has_m             = "DISABLED"
         ,has_z             = "DISABLED"
         ,spatial_reference = arcpy.SpatialReference(g_srid)
         ,config_keyword    = None
      );
      
      arcpy.management.AddFields(
          target
         ,[
             ['Zone'         ,'TEXT'  ,'Zone'     ,254 ,None,'']
            ,['Plumearea'    ,'DOUBLE','Plumearea',None,None,'']
            ,['LINE_ID'      ,'TEXT'  ,'LINE_ID'  ,254 ,None,'']
         ]
      );
      
      if not arcpy.Exists(aprx.homeFolder + os.sep + 'output'):
         os.mkdir(aprx.homeFolder + os.sep + 'output');
      
      if not arcpy.Exists(aprx.homeFolder + os.sep + 'plume_samples.gdb') \
      and    arcpy.Exists(aprx.homeFolder + os.sep + 'plume_samples.gdb.zip'):
         with zipfile.ZipFile(aprx.homeFolder + os.sep + 'plume_samples.gdb.zip',"r") as z:
            z.extractall(aprx.homeFolder);
      
      return;

   def postExecute(self, parameters):
      return;

###############################################################################
class ProcessPlumeScenario(object):

   def __init__(self):
      self.label = "A4 Process Plume Scenario";
      self.description = "";
      self.canRunInBackground = False;

   def getParameterInfo(self):
      
      aprx = arcpy.mp.ArcGISProject(g_prj);
      warn_enb = False;
      warn_str = "";
      
      if arcpy.CheckExtension("Spatial") != "Available":
         warn_enb = True;
         warn_str = "Spatial Analyst license not found";
         
      if not arcpy.Exists(aprx.defaultGeodatabase + os.sep + 'plumeTemplate'):
         warn_enb = True;
         warn_str = "Working not setup.";
         
      project_name = None;
      for i in range(100):
         if not arcpy.Exists(aprx.defaultGeodatabase + os.sep + 'Scenario' + str(i) + '_tracts' ):
            project_name = 'Scenario' + str(i);
            break;
      
      ##---------------------------------------------------------------------##
      param0 = arcpy.Parameter(
          displayName   = ""
         ,name          = "Warnings"
         ,datatype      = "GPString"
         ,parameterType = "Optional"
         ,direction     = "Input"
         ,enabled       = warn_enb
      );
      param0.value = warn_str;
      
      ##---------------------------------------------------------------------##
      param1 = arcpy.Parameter(
          displayName   = "Project Name"
         ,name          = "ProjectName"
         ,datatype      = "GPString"
         ,parameterType = "Required"
         ,direction     = "Input"
         ,enabled       = not warn_enb
      );
      param1.value = project_name;
      
      ##---------------------------------------------------------------------##
      param2 = arcpy.Parameter(
          displayName   = "Overwrite Datasets"
         ,name          = "Overwrite"
         ,datatype      = "GPBoolean"
         ,parameterType = "Required"
         ,direction     = "Input"
         ,enabled       = not warn_enb
      );
      param2.value = False;
      
      ##---------------------------------------------------------------------##
      param3 = arcpy.Parameter(
          displayName   = "Clear Existing Projects from Map"
         ,name          = "ClearExistingProjectsfromMap"
         ,datatype      = "GPBoolean"
         ,parameterType = "Required"
         ,direction     = "Input"
         ,enabled       = not warn_enb
      );
      param3.value = True;
      
      ##---------------------------------------------------------------------##
      param4 = arcpy.Parameter(
          displayName   = "Composite Plume"
         ,name          = "CompositePlume"
         ,datatype      = "GPFeatureRecordSetLayer"
         ,parameterType = "Optional"
         ,direction     = "Input"
         ,enabled       = True
      );
      param4.filter.list = ['Polygon'];
      param4.value = aprx.defaultGeodatabase + os.sep + 'plumeTemplate';
      
      ##---------------------------------------------------------------------##
      param5 = arcpy.Parameter(
          displayName   = "Plume Zone 3"
         ,name          = "PlumeZone3"
         ,datatype      = "GPFeatureRecordSetLayer"
         ,parameterType = "Optional"
         ,direction     = "Input"
         ,enabled       = True
      );
      param5.filter.list = ['Polygon'];
      param5.value = aprx.defaultGeodatabase + os.sep + 'plumeTemplate';
      
      ##---------------------------------------------------------------------##
      param6 = arcpy.Parameter(
          displayName   = "Plume Zone 2"
         ,name          = "PlumeZone2"
         ,datatype      = "GPFeatureRecordSetLayer"
         ,parameterType = "Optional"
         ,direction     = "Input"
         ,enabled       = True
      );
      param6.filter.list = ['Polygon'];
      param6.value = aprx.defaultGeodatabase + os.sep + 'plumeTemplate';
      
      ##---------------------------------------------------------------------##
      param7 = arcpy.Parameter(
          displayName   = "Plume Zone 1"
         ,name          = "PlumeZone1"
         ,datatype      = "GPFeatureRecordSetLayer"
         ,parameterType = "Optional"
         ,direction     = "Input"
         ,enabled       = True
      );
      param7.filter.list = ['Polygon'];
      param7.value = aprx.defaultGeodatabase + os.sep + 'plumeTemplate';
      
      ##---------------------------------------------------------------------##
      param8 = arcpy.Parameter(
          displayName   = "Zoom Percentage"
         ,name          = "ZoomPercentage"
         ,datatype      = "GPDouble"
         ,parameterType = "Optional"
         ,direction     = "Input"
         ,enabled       = True
      );
      param8.value = 10;
      
      ##---------------------------------------------------------------------##
      param9 = arcpy.Parameter(
          displayName   = "Named Basemap Imagery"
         ,name          = "NamedBasemapImagery"
         ,datatype      = "GPString"
         ,parameterType = "Optional"
         ,direction     = "Input"
         ,enabled       = True
      );
      param9.value = "Imagery";
      
      ##---------------------------------------------------------------------##
      param10 = arcpy.Parameter(
          displayName   = "LayerFile Imagery"
         ,name          = "LayerFileImagery"
         ,datatype      = "DELayer"
         ,parameterType = "Optional"
         ,direction     = "Input"
         ,enabled       = True
      );
      
      ##---------------------------------------------------------------------##
      param11 = arcpy.Parameter(
          displayName   = "Raster Service"
         ,name          = "RasterService"
         ,datatype      = "GPRasterLayer"
         ,parameterType = "Optional"
         ,direction     = "Input"
         ,enabled       = False
      );
      
      ##---------------------------------------------------------------------##
      param12 = arcpy.Parameter(
          displayName   = "Tiled Service"
         ,name          = "TiledService"
         ,datatype      = "GPInternetTiledLayer"
         ,parameterType = "Optional"
         ,direction     = "Input"
         ,enabled       = False
      );
      
      return [
          param0
         ,param1
         ,param2
         ,param3
         ,param4
         ,param5
         ,param6
         ,param7
         ,param8
         ,param9
         ,param10
         ,param11
         ,param12
      ];

   def isLicensed(self):
      return True;

   def updateParameters(self, parameters):
   
      if parameters[9].altered and not parameters[9].hasBeenValidated and parameters[9].valueAsText is not None:
         parameters[10].value = "";
         parameters[11].value = "";
         parameters[12].value = "";           
         
      else:
         if parameters[10].altered and not parameters[10].hasBeenValidated and parameters[10].valueAsText is not None:
            parameters[9].value  = "";
            parameters[11].value = "";
            parameters[12].value = ""; 
            
         else:
            if parameters[11].altered and not parameters[11].hasBeenValidated and parameters[11].valueAsText is not None:
               parameters[9].value  = "";
               parameters[10].value = "";
               parameters[12].value = "";                
               
            else:
               if parameters[12].altered and not parameters[12].hasBeenValidated and parameters[12].valueAsText is not None:
                  parameters[9].value  = "";
                  parameters[10].value = ""; 
                  parameters[11].value = ""; 
         
      return;

   def updateMessages(self, parameters):
      return;

   def execute(self, parameters, messages):
   
      def determineNull(pin):
         
         if pin is None:
            return None;
            
         if str(pin) in ["","in_memory\plumeTemplate"]:
            return None;
            
         try:
            jpin = json.loads(pin.JSON);
            isrec = True;
            
         except:
            isrec = False;
           
         if isrec and "features" in jpin:
            if len(jpin["features"]) == 0:
               return None;
            
         return pin;
      
      aprx = arcpy.mp.ArcGISProject(g_prj);
      
      project_name    = parameters[1].valueAsText;
      project_name    = project_name.replace(' ','_');
      project_name    = project_name.replace('-','_');
      project_name    = project_name.replace('.','_');
      project_name    = project_name.replace('#','');
      project_name    = project_name.replace('$','');
      project_name    = project_name.replace(',','');
      project_name    = project_name.replace('=','');
      project_name    = project_name.replace('%','');
      project_name    = project_name.replace('(','');
      project_name    = project_name.replace(')','');
      
      overwrite_boo   = parameters[2].value;
      clear_projects  = parameters[3].value;
      
      composite_plume = determineNull(parameters[4].value);
      zone3_plume     = determineNull(parameters[5].value);
      zone2_plume     = determineNull(parameters[6].value);
      zone1_plume     = determineNull(parameters[7].value);
      
      if  composite_plume is None \
      and zone1_plume is None     \
      and zone2_plume is None     \
      and zone3_plume is None:
         raise ValueError(
            'ERROR: At least one input plume is required for processing'
         );
         
      zoom_percentage = parameters[8].value;
      
      basemap = parameters[9].valueAsText;
      if basemap == "":
         basemap = None;
         
      baselayer = parameters[10].valueAsText;
      if baselayer == "":
         baselayer = None;

      baseraster = parameters[11].valueAsText;
      if baseraster == "":
         baseraster = None;

      basetiled = parameters[12].valueAsText;
      if basetiled == "":
         basetiled = None;          

      #########################################################################
      # Step 10
      # Check for preexisting project
      #########################################################################
      tracts_clip = aprx.defaultGeodatabase + os.sep + project_name + '_tracts';
      plume_file  = aprx.defaultGeodatabase + os.sep + project_name + '_plume';
      
      if arcpy.Exists(tracts_clip) or arcpy.Exists(plume_file) \
      or arcpy.Exists(aprx.homeFolder + os.sep + 'output' + os.sep + project_name):
         if overwrite_boo:
            
            if arcpy.Exists(tracts_clip):
               arcpy.Delete_management(tracts_clip);
            
            if arcpy.Exists(plume_file):
               arcpy.Delete_management(plume_file);
               
            if arcpy.Exists(aprx.homeFolder + os.sep + 'output' + os.sep + project_name):
               arcpy.Delete_management(aprx.homeFolder + os.sep + 'output' + os.sep + project_name);
               
         else:
            raise ValueError(
               'ERROR: The scenario ' + project_name + ' already exists and is currently in use. ' 
             + 'To prevent the loss of critical files, WEST is unable to finish. '
             + 'To continue, either change the scenario name or scenario files from the project geodatabase.'
            );
      
      if not os.path.isdir(aprx.homeFolder + os.sep + 'output' + os.sep + project_name):      
         os.mkdir(aprx.homeFolder + os.sep + 'output' + os.sep + project_name);
      
      #########################################################################
      # Step 20
      # Compose the plume
      #########################################################################     
      if composite_plume is not None:
         z = wash_plume(
            input_plume   = composite_plume
           ,output_name   = plume_file
           ,expected_zone = None
         );
         
      else:
         
         if  zone1_plume is None \
         and zone2_plume is None \
         and zone3_plume is None:
            raise ValueError('Plume input is required');
            
         inputs = [];
         
         if zone3_plume is not None:
            z3 = 'memory/zone3_plume';
            z = wash_plume(
               input_plume   = zone3_plume
              ,output_name   = z3
              ,expected_zone = '3'
            );
            inputs.append(z3);
            
         if zone2_plume is not None:
            z2 = 'memory/zone2_plume';
            z = wash_plume(
               input_plume   = zone2_plume
              ,output_name   = z2
              ,expected_zone = '2'
            );
            inputs.append(z2);
            
         if zone1_plume is not None:
            z1 = 'memory/zone1_plume';
            z = wash_plume(
               input_plume   = zone1_plume
              ,output_name   = z1
              ,expected_zone = '1'
            );
            inputs.append(z1);
            
         arcpy.management.Merge(
             inputs = inputs
            ,output = plume_file
         );       

      #########################################################################
      # Step 30
      # Cleanup multipart rings in single row possibility if below threshold
      #########################################################################
      with arcpy.da.UpdateCursor(
          in_table    = plume_file
         ,field_names = ['Zone','SHAPE@']
         ,sql_clause  = (None, "ORDER BY Zone ASC")
      ) as cursor:
      
         for row in cursor:
            polygon = row[1];
            
            largest_indx = None;
            largest_poly = None;
            if polygon.isMultipart:
            
               # First determine the largest component as the primary plume
               for cnt in range(polygon.partCount):
                  arcpy.AddMessage(str(cnt))
                  part = arcpy.Polygon(
                     polygon.getPart(cnt)
                    ,arcpy.SpatialReference(g_srid)
                  );
                  
                  if largest_poly is None:
                     largest_indx = cnt;
                     largest_poly = part;
                     
                  else:
                     if cnt != largest_indx and part.area > largest_poly.area:
                        largest_indx = cnt;
                        largest_poly = part;
                     
               for cnt in range(polygon.partCount):
                  part = arcpy.Polygon(
                     polygon.getPart(cnt)
                    ,arcpy.SpatialReference(g_srid)
                  );
                  
                  if cnt != largest_indx:
                     
                     if part.disjoint(largest_poly) is False or part.area > (largest_poly.area * 0.01 ):
                        largest_poly = largest_poly.union(part);
                  
               row[1] = largest_poly;
               cursor.updateRow(row);

      #########################################################################
      # Step 40
      # Remove any plume overlaps
      #########################################################################
      plume_zones = [];
      with arcpy.da.SearchCursor(
          in_table    = plume_file
         ,field_names = ['Zone','SHAPE@']
         ,sql_clause  = (None, "ORDER BY Zone ASC")
      ) as cursor:
         
         for row in cursor:
            plume_zones.append((row[0],row[1]));
      
      for item in plume_zones:
      
         with arcpy.da.UpdateCursor(
             in_table    = plume_file
            ,field_names = ['Zone','Plumearea','SHAPE@']
            ,where_clause = "Zone <> " + str(item[0])
            ,sql_clause  = (None, "ORDER BY Zone ASC")
         ) as cursor:
         
            for row in cursor:
            
               if item[0] < row[0]:
                  row[2] = row[2].difference(item[1]);
                  cursor.updateRow(row);
               
      #########################################################################
      # Step 50
      # Redo final plume area sqm value and redo LINE_ID values
      #########################################################################
      with arcpy.da.UpdateCursor(
          in_table    = plume_file
         ,field_names = ['Plumearea','LINE_ID','SHAPE@']
         ,sql_clause  = (None, "ORDER BY Shape_Area ASC")
      ) as cursor:
      
         idx = 1;
         for row in cursor:
         
            row[0] = row[2].getArea("GEODESIC","SQUAREMETERS");
            row[1] = "Level" + f"{idx:03d}"
            cursor.updateRow(row);
            idx += 1;
            
      #########################################################################
      # Step 60
      # Remove any gaps between polygons and tighten up plume coverage
      #########################################################################
      arcpy.Integrate_management(
         in_features = plume_file
      );
      
      #########################################################################
      # Step 40
      # Remove any plume overlaps
      #########################################################################
      plume_outline = 'memory/plume_outline';
      #plume_outline = aprx.defaultGeodatabase + os.sep + 'plume_outline';
      
      if arcpy.Exists(plume_outline):
         arcpy.Delete_management(plume_outline);
      
      arcpy.Dissolve_management(
          in_features       = plume_file
         ,out_feature_class = plume_outline
      );
   
      #########################################################################
      # Step 70
      # Determine tract intersection
      #########################################################################
      arcpy.AddMessage("Determine Tract Intersection.");
      if arcpy.Exists("memory/tractsClipped"):
         arcpy.Delete_management("memory/tractsClipped");
         
      arcpy.analysis.Intersect(
          in_features       = [
             [aprx.defaultGeodatabase + os.sep + 'tracts2010',1]
            ,[plume_file,2]
          ]
         ,out_feature_class = tracts_clip
         ,join_attributes   = "ALL"
      );
      
      flds = [
          'FID_tracts2010'
         ,'STGEOMETRY_AREA'
         ,'STGEOMETRY_LEN'
         ,'FID_' + os.path.basename(plume_file)
      ];
      
      lstFields = arcpy.ListFields(
         dataset = tracts_clip
      );
      
      for field in lstFields:
         if field.name.upper() in [
             'NAME_1'
            ,'OID_'
            ,'SHAPE_LENG'
         ]: 
            flds.append(field.name);    
      
      arcpy.management.DeleteField(
          in_table   = tracts_clip
         ,drop_field = flds
      );
      
      arcpy.management.AddField(
          in_table     = tracts_clip
         ,field_name   = 'Area_m2b'
         ,field_type   = 'DOUBLE'
         ,field_alias  = 'Intersect Area (SqM)'
      );
      
      arcpy.CalculateField_management(
          in_table        = tracts_clip
         ,field           = 'Area_m2b'
         ,expression      = '!Shape!.getArea("GEODESIC","SQUAREMETERS")'
         ,expression_type = "PYTHON"
      );
      
      arcpy.management.AddField(
          in_table     = tracts_clip
         ,field_name   = 'CenTract'
         ,field_type   = 'DOUBLE'
         ,field_alias  = 'CenTract'
      );
      
      arcpy.CalculateField_management(
          in_table        = tracts_clip
         ,field           = 'CenTract'
         ,expression      = '!Area_m2b! / !TRACTAREASQM! * 100'
         ,expression_type = "PYTHON"
      );
      
      #########################################################################
      # Step 80
      # Export results to CSV
      #########################################################################
      arcpy.AddMessage("Export Plume and Tract summaries to CSV.");
      pfolder    = aprx.homeFolder + os.sep + 'output' + os.sep + project_name;
      plume_csv  = pfolder + os.sep + 'plume_area.csv';
      
      if arcpy.Exists(plume_csv):
         arcpy.Delete_management(plume_csv);
         
      plume_area = {};
      with arcpy.da.SearchCursor(
          in_table    = plume_file
         ,field_names = ['Zone','Plumearea']
         ,sql_clause  = (None, "ORDER BY Zone DESC")
      ) as cursor:
      
         with open(plume_csv, mode='w', newline='', encoding='utf-8') as plume:
            plume_writer = csv.writer(plume,delimiter=',',quotechar='"',quoting=csv.QUOTE_MINIMAL);
            
            for row in cursor:
               zone    = row[0];
               areasqm = row[1];
               
               plume_area["Zone" + str(zone)] = areasqm;
               plume_writer.writerow([areasqm]);
      
      # Tract Summary
      tract_file = aprx.defaultGeodatabase + os.sep + project_name + '_tracts';
      tract_csv  = pfolder + os.sep + 'tract_area.csv';
      
      if arcpy.Exists(tract_csv):
         arcpy.Delete_management(tract_csv);
         
      tracts = {};
      with arcpy.da.SearchCursor(
          in_table    = tract_file
         ,field_names = ['GEOID','Zone','CenTract']
         ,sql_clause  = (None, "ORDER BY GEOID,Zone")
      ) as cursor:
      
         for row in cursor:
            if row[0] not in tracts:
               tracts[row[0]] = {};
               
            tracts[row[0]]['Z' + str(row[1])] = row[2];
            
      with open(tract_csv, mode='w', newline='', encoding='utf-8') as tract:
         tract_writer = csv.writer(tract,delimiter=',',quotechar='"',quoting=csv.QUOTE_MINIMAL);
         
         tract_writer.writerow(['Tract','Zone 1','Zone 2','Zone 3']);
         
         for key,val in sorted(tracts.items()):
         
            # Percentages exported in the tract_area.csv file must be between 0 and 1
            # This contrasts with percentages in WEST itself which are between 0 and 100
            if 'Z1' in val:
               val['Z1'] = val['Z1'] / 100;
               z1 = ("%.9f" % val['Z1']).rstrip('0').rstrip('.');
            else:
               z1 = 0;
               
            if 'Z2' in val:
               val['Z2'] = val['Z2'] / 100;
               z2 = ("%.9f" % val['Z2']).rstrip('0').rstrip('.');
            else:
               z2 = 0;
               
            if 'Z3' in val:
               val['Z3'] = val['Z3'] / 100;
               z3 = ("%.9f" % val['Z3']).rstrip('0').rstrip('.');
            else:
               z3 = 0;
               
            tract_writer.writerow([key,z1,z2,z3]);
      
      #########################################################################
      # Step 90
      # Export results to KML
      #########################################################################      
      #scratchz = arcpy.env.scratchFolder   + os.sep + project_name + '.kmz';
      #scratchl = arcpy.env.scratchFolder   + os.sep + 'doc.kml';
      #kml_file = parameters[2].valueAsText + os.sep + project_name + '.kml';
      
      #if arcpy.Exists(scratchz):
      #   arcpy.Delete_management(scratchz);
         
      #if arcpy.Exists(scratchl):
      #   arcpy.Delete_management(scratchl);
      
      #arcpy.conversion.LayerToKML(
      #    layer               = project_name + ' Tracts'
      #   ,out_kmz_file        = scratchz
      #   ,is_composite        = 'NO_COMPOSITE'
      #   ,boundary_box_extent = project_name + ' Tracts'
      #   ,ignore_zvalue       = 'CLAMPED_TO_GROUND'
      #);
      
      #if arcpy.Exists(kml_file):
      #   arcpy.Delete_management(kml_file);
         
      #with zipfile.ZipFile(scratchz,"r") as zip_ref:
      #   zip_ref.extract('doc.kml',arcpy.env.scratchFolder);
         
      #with open(aprx.homeFolder + os.sep + 'ColorCode.txt',"r") as myfile:
      #   data = myfile.read();
         
      #with open(scratchl, mode="r") as curs_in:
      #   with open(kml_file, mode='w', newline='', encoding='utf-8') as curs_out:
         
      #      lines = curs_in.readlines();
      #      for line in lines:
      #         curs_out.write(line);
               
      #         if line.startswith('  <name>' + project_name + ' Tracts</name>'):
      #            curs_out.write(data);
      
      #########################################################################
      # Step 100
      # Add scenario to map
      #########################################################################
      arcpy.AddMessage("Add scenario to map.");
      map = aprx.listMaps(g_mapname)[0];
      
      if clear_projects:
         for lyr in map.listLayers():
            if lyr.supports("name"):
               if lyr.name.find(' Plume') > -1 or lyr.name.find(' Tracts') > -1:
                  map.removeLayer(lyr);
      
      lyrx_plume = tempLyrx(
          in_layerfile = aprx.homeFolder + os.sep + 'plume.lyrx'
         ,dataset      = os.path.basename(plume_file)
         ,name         = project_name + ' Plume'
         ,aprx         = aprx
      );
      lyr_plume = arcpy.mp.LayerFile(lyrx_plume);
      map.addLayer(lyr_plume,"TOP");
      
      lyrx_tracts = tempLyrx(
          in_layerfile = aprx.homeFolder + os.sep + 'tracts.lyrx'
         ,dataset      = os.path.basename(tracts_clip)
         ,name         = project_name + ' Tracts'
         ,aprx         = aprx
      );
      lyr_tracts = arcpy.mp.LayerFile(lyrx_tracts);
      map.addLayer(lyr_tracts,"TOP");
      
      #########################################################################
      # Step 110
      # Adjust basemap per inputs
      #########################################################################
      if basemap is not None:
         map.addBasemap(basemap);
         
      else:
         for lyr in map.listLayers():
            if lyr.isBasemapLayer:
               map.removeLayer(lyr);
                  
         if baselayer is not None:   
            lyr_baselayer = arcpy.mp.LayerFile(baselayer);
            map.addLayer(lyr_baselayer,"BOTTOM");
            
         else:
         
            if baseraster is not None:
               result = arcpy.MakeImageServerLayer_management(
                   in_image_service = baseraster
                  ,out_imageserver_layer = 'Raster Service'
               );
               lyr = result.getOutput(0);
               map.addLayer(lyr,"BOTTOM");
               
            else:
            
               if basetiled is not None:
                  result = arcpy.MakeFeatureLayer_management(
                      in_features = basetiled
                     ,out_layer   = 'Tiled Service'
                  );
                  lyr = result.getOutput(0);
                  map.addLayer(lyr,"BOTTOM");
                  
               else:
                  raise ValueError('Imagery base layer required');
            
      #########################################################################
      # Step 120
      # Turn off tract labeling
      #########################################################################
      for lyr in map.listLayers():
         if lyr.supports("name"):
            if lyr.name.find(' Tracts') > -1:
               lyr.showLabels = False;      
      
      #########################################################################
      # Step 130
      # Zoom to plume
      #########################################################################
      mv = aprx.activeView;
      desc = arcpy.Describe(plume_file);
      plume_extent = desc.extent;
      
      if zoom_percentage is not None:
      
         plume_array = arcpy.Array();
         plume_array.add(plume_extent.lowerLeft);
         plume_array.add(plume_extent.lowerRight);
         plume_array.add(plume_extent.upperRight);
         plume_array.add(plume_extent.upperLeft);
         plume_array.add(plume_extent.lowerLeft);
         
         if plume_extent.width > plume_extent.height:
            plume_girth = plume_extent.width;
         else:
            plume_girth = plume_extent.height;
            
         plume_dist = plume_girth * (zoom_percentage * .01);
         
         if arcpy.Exists("memory/polyBuff"):
            arcpy.Delete_management("memory/polyBuff");

         plume_poly = arcpy.Polygon(plume_array,g_srid);
         arcpy.Buffer_analysis(plume_poly,"memory/polyBuff",plume_dist,"OUTSIDE_ONLY");
         
         desc = arcpy.Describe("memory/polyBuff");
         plume_extent = desc.extent;
      
      mv.camera.setExtent(plume_extent);
      
      #########################################################################
      # Step 140
      # Process plume basemap tiffs
      #########################################################################
      arcpy.AddMessage("Process plume basemap tiffs.");
      
      sigFile = aprx.homeFolder + os.sep + '061818v1.gsg';
      probThreshold = "0.0";
      aPrioriWeight = "EQUAL";
      
      Zone_list = {
          "Zone1": [0,0,0]
         ,"Zone2": [0,0,0]
         ,"Zone3": [0,0,0]
      };
      Compatible_Surfaces = ['Soil','Concrete','Asphalt','Vegetation','Water'];
      
      imap = aprx.listMaps(g_imagemap)[0];
      
      for lyr in imap.listLayers():
         imap.removeLayer(lyr);
         
      if basemap is not None:
         imap.addBasemap(basemap);
         
      else:
         if baselayer is not None:
            lyr_baselayer = arcpy.mp.LayerFile(baselayer);
            imap.addLayer(lyr_baselayer,"BOTTOM");
            
         else:
         
            if baseraster is not None:
               result = arcpy.MakeImageServerLayer_management(
                   in_image_service = baseraster
                  ,out_imageserver_layer = 'Raster Service'
               );
               lyr = result.getOutput(0);
               imap.addLayer(lyr,"BOTTOM");
               
            else:
            
               if basetiled is not None:
                  result = arcpy.MakeImageServerLayer_management(
                      in_image_service = basetiled
                     ,out_imageserver_layer = 'Tiled Service'
                  );
                  lyr = result.getOutput(0);
                  map.addLayer(lyr,"BOTTOM");
      
      lyt = aprx.listLayouts(g_imagemap)[0];
      mf = lyt.listElements("MAPFRAME_ELEMENT",g_imagemap)[0];
      
      with arcpy.da.SearchCursor(
          in_table    = plume_file
         ,field_names = ['Zone','SHAPE@']
         ,sql_clause  = (None, "ORDER BY Zone ASC")
      ) as cursor:
         
         for row in cursor:
            zone_val = row[0];
            zone_shp = row[1];
            
            if zone_val < 4:
            
               mf.camera.setExtent(zone_shp.extent);
               
               target = aprx.homeFolder + os.sep + 'output' + os.sep + project_name + os.sep + 'Zone' + str(zone_val);
               
               if arcpy.Exists(target + '.tif'):
                  arcpy.Delete_management(target + '.tif');
               
               mf.exportToTIFF(
                   out_tif             = target + '.tif'
                  ,resolution          = 96
                  ,world_file          = False
                  ,color_mode          = "24-BIT_TRUE_COLOR"
                  ,tiff_compression    = "NONE"
                  ,geoTIFF_tags        = True
                  ,embed_color_profile = True
               );
               
               if arcpy.Exists(target + '.bmp'):
                  arcpy.Delete_management(target + '.bmp');
               
               arcpy.management.Clip(
                   in_raster           = target + '.tif'
                  ,rectangle           = "#"
                  ,out_raster          = target + '.bmp'
                  ,in_template_dataset = zone_shp
                  ,nodata_value        = 255
                  ,clipping_geometry   = "ClippingGeometry"
                  ,maintain_clipping_extent = "NO_MAINTAIN_EXTENT"
               );
               
               if arcpy.Exists(target + '.img'):
                  arcpy.Delete_management(target + '.img');
                  
               mlcOut = MLClassify(
                   in_raster_bands        = target + '.bmp'
                  ,in_signature_file      = sigFile
                  ,reject_fraction        = probThreshold
                  ,a_priori_probabilities = aPrioriWeight
               );
               mlcOut.save(target + '.img');

               pixel_total = 0;
               with arcpy.da.SearchCursor(
                   in_table    = target + '.img'
                  ,field_names = ["Count"]
               ) as inner_cursor:
               
                  for inner_row in inner_cursor:
                     pixel_total += inner_row[0];
                  
               arcpy.AddMessage(".  Zone" + str(zone_val) + " has " + str(pixel_total) + " pixels.");
               
               arcpy.management.AddField(
                   in_table   = target + '.img'
                  ,field_name = 'Zone' + str(zone_val)
                  ,field_type = 'DOUBLE'
               );
               
               with arcpy.da.UpdateCursor(
                   in_table    = target + '.img'
                  ,field_names = ["Count", "Zone" + str(zone_val), "CLASSNAME"]
               ) as inner_cursor:
               
                  for inner_row in inner_cursor:
                  
                     inner_row[1] = inner_row[0] / pixel_total;
                     inner_cursor.updateRow(inner_row);
                     
                     if inner_row[2] in Compatible_Surfaces[0]:
                        Zone_list["Zone" + str(zone_val)][0] = inner_row[1];
                     if inner_row[2] in Compatible_Surfaces[1]:
                        Zone_list["Zone" + str(zone_val)][1] = inner_row[1];
                     if inner_row[2] in Compatible_Surfaces[2]:
                        Zone_list["Zone" + str(zone_val)][2] = inner_row[1];
                     # Note that this next row adds vegetation results to soil since WEST doesn't account for vegetation yet; remove plus to fix this
                     if inner_row[2] in Compatible_Surfaces[3]:
                        Zone_list["Zone" + str(zone_val)][0] += inner_row[1];
                     # The next row is turned off since WEST doesn't account for water. The % is still taken from the total. 
                     # To account for water, expand the list (add another zero to zone list, add water to compatible surfaces and remove the comment below
                     #if inner_row[2] in Compatible_Surfaces[4]: Zone1_list[4] = inner_row[1] 

      #multiply by 100 for whole number

      Zone_list["Zone1"] = [i * 100 for i in Zone_list["Zone1"]];
      Zone_list["Zone2"] = [i * 100 for i in Zone_list["Zone2"]];
      Zone_list["Zone3"] = [i * 100 for i in Zone_list["Zone3"]];

      #reconfigure lists to match required WEST inputs. This can be modified for future compatibility

      image_analysis_line1 = [''            ,'Zone1'              ,'Zone2'              ,'Zone3'];
      image_analysis_line2 = ['Asphalt (%)' ,Zone_list["Zone1"][2],Zone_list["Zone2"][2],Zone_list["Zone3"][2]];
      image_analysis_line3 = ['Concrete (%)',Zone_list["Zone1"][1],Zone_list["Zone2"][1],Zone_list["Zone3"][1]];
      image_analysis_line4 = ['Soil (%)'    ,Zone_list["Zone1"][0],Zone_list["Zone2"][0],Zone_list["Zone3"][0]];
      
      target = aprx.homeFolder + os.sep + 'output' + os.sep + project_name + os.sep + 'ground_surface_data.csv';
      
      if arcpy.Exists(target):
         arcpy.Delete_management(target);
      
      with open(
          target
         ,mode     = 'w'
         ,newline  = ''
         ,encoding = 'utf-8'
      ) as myfile:
         wr = csv.writer(
             myfile
            ,quotechar = '|'
            ,quoting   = csv.QUOTE_MINIMAL
         );
         wr.writerow(image_analysis_line1);
         wr.writerow(image_analysis_line2);
         wr.writerow(image_analysis_line3);
         wr.writerow(image_analysis_line4);

      arcpy.AddMessage("Image processing complete.");

      #########################################################################
      # Step 140
      # Process plume structures polygons
      #########################################################################
      has_USA_Structures = False;
      for lyr in map.listLayers():
         if lyr.supports("name"):
            if lyr.name == 'USA_Structures':
               has_USA_Structures = True;
      
      if not has_USA_Structures:
         arcpy.AddMessage("USA Structures Living Atlas layer not found, skipping.");
      
      else:
         arcpy.AddMessage("Pulling USA Structures polygons by plume.");
      
         arcpy.management.SelectLayerByLocation(
             in_layer        = 'USA_Structures'
            ,overlap_type    = 'INTERSECT'
            ,select_features = plume_outline
            ,selection_type  = 'NEW_SELECTION'
         );
         
         usas_scrape = arcpy.env.scratchGDB + os.sep + 'usa_structures';
         if arcpy.Exists(usas_scrape):
            arcpy.Delete_management(usas_scrape);
         
         arcpy.management.CopyFeatures(
             in_features       = 'USA_Structures'
            ,out_feature_class = usas_scrape
         );
         
      #########################################################################
      # Step 140
      # Spatial join structures against uncut tracts and output structures dump
      #########################################################################
      if has_USA_Structures:
      
         usas_bytract = arcpy.env.scratchGDB + os.sep + 'usa_bytract';
         if arcpy.Exists(usas_bytract):
            arcpy.Delete_management(usas_bytract);
         
         arcpy.analysis.SpatialJoin(
             target_features   = usas_scrape
            ,join_features     = aprx.defaultGeodatabase + os.sep + 'tracts2010'
            ,out_feature_class = usas_bytract
            ,join_operation    = 'JOIN_ONE_TO_MANY'
            ,join_type         = 'KEEP_ALL'
            ,field_mapping     = 'BUILD_ID "BUILD_ID" true true false 4 Long 0 0,First,#,' + usas_scrape + ',BUILD_ID,-1,-1;' + 
                                 'OCC_CLS "OCC_CLS" true true false 20 Text 0 0,First,#,' + usas_scrape + ',OCC_CLS,0,20;' + 
                                 'PRIM_OCC "PRIM_OCC" true true false 35 Text 0 0,First,#,' + usas_scrape + ',PRIM_OCC,0,35;' + 
                                 'SEC_OCC "SEC_OCC" true true false 13 Text 0 0,First,#,' + usas_scrape + ',SEC_OCC,0,13;' + 
                                 'HEIGHT "HEIGHT" true true false 4 Float 0 0,First,#,' + usas_scrape + ',HEIGHT,-1,-1;' + 
                                 'GEOID "GEOID" true true false 11 Text 0 0,First,#,' + aprx.defaultGeodatabase + os.sep + 'tracts2010,GEOID,0,11;' +
                                 'TRACTAREASQM "Tract Area (SqM)" true true false 8 Double 0 0,First,#,' + aprx.defaultGeodatabase + os.sep + 'tracts2010,TRACTAREASQM,-1,-1'
            ,match_option      = 'INTERSECT'
         );
         
         target = aprx.homeFolder + os.sep + 'output' + os.sep + project_name + os.sep + 'custom_infrastructure.csv';
         
         if arcpy.Exists(target):
            arcpy.Delete_management(target);
         
         with open(target,mode='w',newline='',encoding='utf-8') as usas_bytract_handle:
            usas_bytract_writer = csv.writer(usas_bytract_handle,delimiter=',',quotechar='"',quoting=csv.QUOTE_MINIMAL);
            
            usas_bytract_writer.writerow(['Tract_ID','Type','Floors','Area','Tract_Area','Height']);
         
            with arcpy.da.SearchCursor(
                in_table    = usas_bytract
               ,field_names = ['GEOID','TRACTAREASQM','BUILD_ID','OCC_CLS','PRIM_OCC','HEIGHT','SHAPE@']
               ,sql_clause  = (None, "ORDER BY GEOID,OCC_CLS,BUILD_ID")
            ) as cursor:
            
               for row in cursor:
                  tract_id    = row[0];
                  tract_area  = row[1];
                  building_id = row[2];
                  occ_cls     = row[3];
                  prim_occ    = row[4];
                  bheight     = row[5];
                  shape       = row[6];
                  
                  if bheight is None:
                     bheight_str = 'Null';
                  else:
                     bheight_str = str(bheight);
                     
                  if occ_cls is None:
                     occ_cls = 'Null';
                     
                  if prim_occ is None:
                     prim_occ = 'Null';
                     
                  build_area  = shape.getArea('GEODESIC','SquareMeters');
                  
                  usas_bytract_writer.writerow([tract_id,occ_cls,'Null',build_area,tract_area,bheight_str]);
       
      #########################################################################
      # Step 140
      # Spatial join structures against zones for area totals
      #########################################################################
      build_byzone_totals = {};
      build_byzone_counts = {};
      
      if has_USA_Structures:
      
         usas_byzone = arcpy.env.scratchGDB + os.sep + 'usas_byzone';
         if arcpy.Exists(usas_byzone):
            arcpy.Delete_management(usas_byzone);
         
         arcpy.analysis.SpatialJoin(
             target_features   = usas_scrape
            ,join_features     = plume_file
            ,out_feature_class = usas_byzone
            ,join_operation    = 'JOIN_ONE_TO_MANY'
            ,join_type         = 'KEEP_ALL'
            ,field_mapping     = 'BUILD_ID "BUILD_ID" true true false 4 Long 0 0,First,#,' + usas_scrape + ',BUILD_ID,-1,-1;' + 
                                 'OCC_CLS "OCC_CLS" true true false 20 Text 0 0,First,#,' + usas_scrape + ',OCC_CLS,0,20;' + 
                                 'PRIM_OCC "PRIM_OCC" true true false 35 Text 0 0,First,#,' + usas_scrape + ',PRIM_OCC,0,35;' + 
                                 'SEC_OCC "SEC_OCC" true true false 13 Text 0 0,First,#,' + usas_scrape + ',SEC_OCC,0,13;' + 
                                 'HEIGHT "HEIGHT" true true false 4 Float 0 0,First,#,' + usas_scrape + ',HEIGHT,-1,-1;' + 
                                 'Zone "Zone" true true false 4 Long 0 0,First,#,' + plume_file + ',Zone,-1,-1'
            ,match_option      = 'INTERSECT'
         );
         
         with arcpy.da.SearchCursor(
             in_table    = usas_byzone
            ,field_names = ['BUILD_ID','OCC_CLS','PRIM_OCC','HEIGHT','Zone','SHAPE@']
            ,sql_clause  = (None, "ORDER BY Zone,OCC_CLS,BUILD_ID")
         ) as cursor:
         
            for row in cursor:
               build_id   = row[0];
               occ_cls    = row[1];
               if occ_cls is None:
                  occ_cls = 'Null';
                  
               prim_occ   = row[2];
               if prim_occ is None:
                  prim_occ = 'Null';
                  
               bheight    = row[3];
               zone       = row[4];
               shape      = row[5];
               build_area = shape.getArea('GEODESIC','SquareMeters');
               
               if occ_cls + ',' + str(zone) not in build_byzone_totals:
                  build_byzone_totals[occ_cls + ',' + str(zone)] = build_area;
                  build_byzone_counts[occ_cls + ',' + str(zone)] = 1;
                  
               else:
                  build_byzone_totals[occ_cls + ',' + str(zone)] += build_area;
                  build_byzone_counts[occ_cls + ',' + str(zone)] += 1;
                  
      #########################################################################
      # Step 140
      # Spatial join structures against zone-cut tracts
      #########################################################################
      if has_USA_Structures:
      
         usas_join = arcpy.env.scratchGDB + os.sep + 'usa_join';
         if arcpy.Exists(usas_join):
            arcpy.Delete_management(usas_join);
         
         arcpy.analysis.SpatialJoin(
             target_features   = usas_scrape
            ,join_features     = tract_file
            ,out_feature_class = usas_join
            ,join_operation    = 'JOIN_ONE_TO_MANY'
            ,join_type         = 'KEEP_ALL'
            ,field_mapping     = 'BUILD_ID "BUILD_ID" true true false 4 Long 0 0,First,#,' + usas_scrape + ',BUILD_ID,-1,-1;' + 
                                 'OCC_CLS "OCC_CLS" true true false 20 Text 0 0,First,#,' + usas_scrape + ',OCC_CLS,0,20;' + 
                                 'PRIM_OCC "PRIM_OCC" true true false 35 Text 0 0,First,#,' + usas_scrape + ',PRIM_OCC,0,35;' + 
                                 'SEC_OCC "SEC_OCC" true true false 13 Text 0 0,First,#,' + usas_scrape + ',SEC_OCC,0,13;' + 
                                 'HEIGHT "HEIGHT" true true false 13 Float 0 0,First,#,' + usas_scrape + ',SEC_OCC,0,13;' + 
                                 'GEOID "GEOID" true true false 11 Text 0 0,First,#,Scenario22 Tracts,GEOID,0,11;' + 
                                 'TRACTAREASQM "Tract Area (SqM)" true true false 8 Double 0 0,First,#,' + tract_file + ',TRACTAREASQM,-1,-1;'
                                 'Zone "Zone" true true false 4 Long 0 0,First,#,' + tract_file + ',Zone,-1,-1'
            ,match_option      = 'INTERSECT'
         );
         
      #########################################################################
      # Step 140
      # Process plume structures polygons
      #########################################################################
      if has_USA_Structures:
         
         cnt = {};
         sqm = {};
         lop = {};
         occ = {};
         occ_cls_totals = {};
         with arcpy.da.SearchCursor(
             in_table    = usas_join
            ,field_names = ['GEOID','Zone','OCC_CLS','PRIM_OCC','SHAPE@']
            ,sql_clause  = (None, "ORDER BY GEOID,Zone,OCC_CLS,PRIM_OCC")
         ) as outer_cursor:
            
            for outer_row in outer_cursor:
            
               if outer_row[1] < 4:
                  tractid  = outer_row[0];
                  zone_key = 'Zone' + str(outer_row[1]);
                  occ_cls  = outer_row[2];
                  prim_occ = outer_row[3];
                  shape    = outer_row[4];
                  
                  if tractid not in cnt:
                     cnt[tractid] = {
                         'Zone1': {}
                        ,'Zone2': {}
                        ,'Zone3': {}
                     };
                     sqm[tractid] = {
                         'Zone1': {}
                        ,'Zone2': {}
                        ,'Zone3': {}
                     };
                     lop[tractid] = {};
                  
                  if occ_cls is None:
                     occ_cls = 'Null';
                     
                  if occ_cls not in occ:
                     occ[occ_cls] = 1;
                  
                  if prim_occ is None:
                     prim_occ = 'Null';
                        
                  if occ_cls not in cnt[tractid][zone_key]:
                     cnt[tractid][zone_key][occ_cls] = {};
                     sqm[tractid][zone_key][occ_cls] = {};
                  
                  if prim_occ not in cnt[tractid][zone_key][occ_cls]:
                     cnt[tractid][zone_key][occ_cls][prim_occ] = 1;
                     sqm[tractid][zone_key][occ_cls][prim_occ] = shape.getArea('GEODESIC','SquareMeters');
                     
                  else:
                     cnt[tractid][zone_key][occ_cls][prim_occ] += 1;
                     sqm[tractid][zone_key][occ_cls][prim_occ] += shape.getArea('GEODESIC','SquareMeters');
                     
                  if occ_cls + ',' + prim_occ not in lop[tractid]:
                     lop[tractid][occ_cls + ',' + prim_occ] = [occ_cls,prim_occ];

         target = aprx.homeFolder + os.sep + 'output' + os.sep + project_name + os.sep + 'usa_structures.csv';
         
         if arcpy.Exists(target):
            arcpy.Delete_management(target);
         
         with open(target,mode='w',newline='',encoding='utf-8') as usa_structures:
            usas_writer = csv.writer(usa_structures,delimiter=',',quotechar='"',quoting=csv.QUOTE_MINIMAL);
            
            usas_writer.writerow(['Tract','OCC_CLS','PRIM_OCC','Zone1_Cnt','Zone1_SqM','Zone2_Cnt','Zone2_SqM','Zone3_Cnt','Zone3_SqM']);
        
            for key1,val1 in sorted(cnt.items()):
               tractid = key1;
               
               for key2,val2 in sorted(lop[tractid].items()):
                  occ_cls  = val2[0];
                  prim_occ = val2[1];
                  
                  if occ_cls in cnt[tractid]['Zone1'] and prim_occ in cnt[tractid]['Zone1'][occ_cls]:
                     zone1_cnt = cnt[tractid]['Zone1'][occ_cls][prim_occ];
                     zone1_sqm = sqm[tractid]['Zone1'][occ_cls][prim_occ];
                  else:
                     zone1_cnt = 0;
                     zone1_sqm = 0;
                     
                  if occ_cls in cnt[tractid]['Zone2'] and prim_occ in cnt[tractid]['Zone2'][occ_cls]:
                     zone2_cnt = cnt[tractid]['Zone2'][occ_cls][prim_occ];
                     zone2_sqm = sqm[tractid]['Zone2'][occ_cls][prim_occ];
                  else:
                     zone2_cnt = 0;
                     zone2_sqm = 0;
               
                  if occ_cls in cnt[tractid]['Zone3'] and prim_occ in cnt[tractid]['Zone3'][occ_cls]:
                     zone3_cnt = cnt[tractid]['Zone3'][occ_cls][prim_occ];
                     zone3_sqm = sqm[tractid]['Zone3'][occ_cls][prim_occ];
                  else:
                     zone3_cnt = 0;
                     zone3_sqm = 0;
                     
                  usas_writer.writerow([tractid,occ_cls,prim_occ,zone1_cnt,zone1_sqm,zone2_cnt,zone2_sqm,zone3_cnt,zone3_sqm]);
      
      #########################################################################
      # Step 140
      # Generate final summary report
      #########################################################################
      dest_filename = aprx.homeFolder + os.sep + 'output' + os.sep + project_name + os.sep + 'project_summary.xlsx';
      
      if arcpy.Exists(dest_filename):
         arcpy.Delete_management(dest_filename);
         
      wb = Workbook();
      rpt = wb.active;
      rpt.title = 'Summary';
      
      bld  = Font(size=9,bold=True);
      ft9  = Font(size=9);
      
      rpt.column_dimensions['A'].width = 12;
      
      rpt['A1']  = "Project/Scenario Name:"
      rpt['A1'].font = bld;
      
      rpt['C1']  = project_name;
      rpt['C1'].font = ft9;
      
      rpt.row_dimensions[1].height  = 12;
      rpt.row_dimensions[2].height  = 12;
      rpt.row_dimensions[3].height  = 12;
      rpt.row_dimensions[4].height  = 12;
      rpt.row_dimensions[5].height  = 12;
      rpt.row_dimensions[6].height  = 12;
      rpt.row_dimensions[7].height  = 12;
      rpt.row_dimensions[8].height  = 12;
      rpt.row_dimensions[9].height  = 12;
      rpt.row_dimensions[10].height = 12;
      rpt.row_dimensions[11].height = 12;
      rpt.row_dimensions[12].height = 12;
      rpt.row_dimensions[13].height = 12;
      rpt.row_dimensions[14].height = 12;
      rpt.row_dimensions[15].height = 12;
      rpt.row_dimensions[16].height = 12;
      rpt.row_dimensions[17].height = 12;
      rpt.row_dimensions[18].height = 12;
      rpt.row_dimensions[19].height = 12;
      rpt.row_dimensions[20].height = 12;
      rpt.row_dimensions[21].height = 12;
      rpt.row_dimensions[22].height = 12;
      rpt.row_dimensions[23].height = 12;
      rpt.row_dimensions[24].height = 12;
      rpt.row_dimensions[25].height = 12;
      
      rpt['A3']  = "Total Affected Area (m2):";
      rpt['A3'].font = ft9;
      rpt['A4']  = "Zone 1 (m2):";
      rpt['A4'].font = ft9;
      rpt['A5']  = "Zone 2 (m2):";
      rpt['A5'].font = ft9;
      rpt['A6']  = "Zone 3 (m2):";
      rpt['A6'].font = ft9;
      rpt['B4']  = plume_area["Zone1"];
      rpt['B4'].font = ft9;
      rpt['B5']  = plume_area["Zone2"];
      rpt['B5'].font = ft9;
      rpt['B6']  = plume_area["Zone3"];
      rpt['B6'].font = ft9;
      
      rpt['A8']  = "Surface Materials";
      rpt['A8'].font = bld;
      rpt['A10'] = "Zone 1";
      rpt['A10'].font = ft9;
      rpt['A11'] = "Zone 2";
      rpt['A11'].font = ft9;
      rpt['A12'] = "Zone 3";
      rpt['A12'].font = ft9;
      rpt['B9']  = "Soil (%)";
      rpt['B9'].font = ft9;
      rpt['B10'] = Zone_list["Zone1"][0];
      rpt['B10'].font = ft9;
      rpt['B11'] = Zone_list["Zone2"][0];
      rpt['B11'].font = ft9;
      rpt['B12'] = Zone_list["Zone3"][0];
      rpt['B12'].font = ft9;
      rpt['C9']  = "Concrete (%)";
      rpt['C9'].font = ft9;
      rpt['C10'] = Zone_list["Zone1"][1];
      rpt['C10'].font = ft9;
      rpt['C11'] = Zone_list["Zone2"][1];
      rpt['C11'].font = ft9;
      rpt['C12'] = Zone_list["Zone3"][1];
      rpt['C12'].font = ft9;
      rpt['D9']  = "Asphalt (%)";
      rpt['D9'].font = ft9;
      rpt['D10'] = Zone_list["Zone1"][2];
      rpt['D10'].font = ft9;
      rpt['D11'] = Zone_list["Zone2"][2];
      rpt['D11'].font = ft9;
      rpt['D12'] = Zone_list["Zone3"][2];
      rpt['D12'].font = ft9;
      
      rpt['A14'] = "Buildings";
      rpt['A14'].font = bld;
      rpt['A17'] = "Zone 1"
      rpt['A17'].font = ft9;
      rpt['A18'] = "Zone 2"
      rpt['A18'].font = ft9;
      rpt['A19'] = "Zone 3"
      rpt['A19'].font = ft9;
      
      if has_USA_Structures:
         col = 2;
         for key,val in sorted(occ.items()):
            
            let1 = get_column_letter(col);
            rpt[let1 + '15'] = key;
            rpt[let1 + '15'].font = ft9;
            rpt[let1 + '16'] = "Count";
            rpt[let1 + '16'].font = ft9;
            let2 = get_column_letter(col + 1);
            rpt[let2 + '16'] = "Footprint (m2)";
            rpt[let2 + '16'].font = ft9;
            
            if key + ',1' in build_byzone_totals:
               rpt[let1 + '17'] = build_byzone_counts[key + ',1'];
               rpt[let1 + '17'].font = ft9;
               rpt[let2 + '17'] = build_byzone_totals[key + ',1'];
               rpt[let2 + '17'].font = ft9;
            else:
               rpt[let1 + '17'] = 0;
               rpt[let1 + '17'].font = ft9;
               rpt[let2 + '17'] = 0;
               rpt[let2 + '17'].font = ft9;
               
            if key + ',2' in build_byzone_totals:
               rpt[let1 + '18'] = build_byzone_counts[key + ',2'];
               rpt[let1 + '18'].font = ft9;
               rpt[let2 + '18'] = build_byzone_totals[key + ',2'];
               rpt[let2 + '18'].font = ft9;
            else:
               rpt[let1 + '18'] = 0;
               rpt[let1 + '18'].font = ft9;
               rpt[let2 + '18'] = 0;
               rpt[let2 + '18'].font = ft9;
               
            if key + ',3' in build_byzone_totals:
               rpt[let1 + '19'] = build_byzone_counts[key + ',3'];
               rpt[let1 + '19'].font = ft9;
               rpt[let2 + '19'] = build_byzone_totals[key + ',3'];
               rpt[let2 + '19'].font = ft9;
            else:
               rpt[let1 + '19'] = 0;
               rpt[let1 + '19'].font = ft9;
               rpt[let2 + '19'] = 0;
               rpt[let2 + '19'].font = ft9;
               
            rpt.column_dimensions[let1].width = 12;
            rpt.column_dimensions[let2].width = 12;
               
            col += 2;
      
      wb.save(dest_filename);
      
      return;

   def postExecute(self, parameters):
      return;
      
###############################################################################
def tempLyrx(
    in_layerfile
   ,dataset   = None
   ,sourceURI = None
   ,name      = None
   ,aprx      = None
):

   if aprx is None:
      aprx = arcpy.mp.ArcGISProject(g_prj);

   with open(in_layerfile,"r") as jsonFile_target:
      data_in = json.load(jsonFile_target);

   for item in data_in["layerDefinitions"]:
      
      if dataset is not None:   
         item["featureTable"]["dataConnection"]["workspaceConnectionString"] = "DATABASE=" + aprx.defaultGeodatabase;
         item["featureTable"]["dataConnection"]["dataset"] = dataset;

      elif sourceURI is not None:
         item["sourceURI"] = sourceURI;
      
      if name is not None:
         item["name"] = name;
   
   lyrx_target = arcpy.CreateScratchName(
       prefix    = "tmp"
      ,suffix    = ".lyrx"
      ,data_type = "Folder"
      ,workspace = arcpy.env.scratchFolder
   );
   
   with open(lyrx_target,"w") as jsonFile:
      json.dump(data_in,jsonFile);

   return lyrx_target;

###############################################################################
def wash_plume(
   input_plume
  ,output_name
  ,expected_zone = None
):

   if arcpy.Exists(output_name):
      arcpy.Delete_management(output_name);
      
   desc = arcpy.Describe(input_plume);
   if desc.shapeType != 'Polygon':
      raise ValueError("ERROR: input plumes must be polygons.");
   in_srid = desc.spatialReference.factoryCode;
   
   if in_srid != g_srid:
   
     arcpy.management.Project(
         in_dataset         = input_plume
        ,out_dataset        = output_name
        ,out_coor_system    = arcpy.SpatialReference(g_srid)
     );
     
   else:
   
      arcpy.CopyFeatures_management(
          in_features       = input_plume 
         ,out_feature_class = output_name
      );
   
   lstFields = arcpy.ListFields(
      dataset = output_name
   );
   
   has_zone_fld     = False;
   chk_good_zone    = False;
   chk_fix_zone     = False;
   
   has_bad_zone_type = False;
   
   has_plume_area   = False;
   chk_good_area    = False;
   chk_fix_area     = False;
   
   has_line_id_fld  = False;
   chk_good_line_id = False;
   chk_fix_line_id  = False;
   
   for field in lstFields:
      if field.name == "Zone" and field.type == "Integer":
         has_zone_fld   = True;
         chk_good_zone  = True;

      if not chk_good_zone and field.name.upper() == "ZONE" and field.type == "Integer":
         has_zone_fld   = True;
         chk_fix_zone   = True;
         fix_zone_fld   = field.name;
         
      if not chk_good_zone and not chk_fix_zone and field.name.upper() == "ZONE" and field.type != "Integer":
         has_bad_zone_type = True;
         fix_zone_fld   = field.name;
         
      if field.name == "Plumearea":
         has_plume_area = True;
         chk_good_area  = True;
         
      if not chk_good_area and field.name.upper() == "PLUMEAREA":
         has_plume_area = True;
         chk_fix_area   = True;
         fix_area_fld   = field.name;
         
      if field.name == "LINE_ID":
         has_line_id_fld  = True;
         chk_good_line_id = True;
         
      if not chk_good_line_id and field.name.upper() == "LINE_ID":
         has_line_id_fld = True;
         chk_fix_line_id = True;
         fix_line_id_fld = field.name;

   if has_zone_fld and chk_fix_zone:
   
      arcpy.management.AlterField(
          in_table        = output_name
         ,field           = fix_zone_fld
         ,new_field_name  = 'xxdzxx'
      );
      
      arcpy.management.AlterField(
          in_table        = output_name
         ,field           = 'xxdzxx'
         ,new_field_name  = 'Zone'
         ,new_field_alias = 'Zone'
      );
      
   if has_plume_area and chk_fix_area:
   
      arcpy.management.AlterField(
          in_table        = output_name
         ,field           = fix_area_fld
         ,new_field_name  = 'xxdzxx'
      );
      
      arcpy.management.AlterField(
          in_table        = output_name
         ,field           = 'xxdzxx'
         ,new_field_name  = 'Plumearea'
         ,new_field_alias = 'Plume Area (SqM)'
      );
      
   if has_bad_zone_type:
   
      arcpy.management.AlterField(
          in_table        = output_name
         ,field           = fix_zone_fld
         ,new_field_name  = 'xxdzxx'
      );
      
      arcpy.management.AddField(
          in_table        = output_name
         ,field_name      = 'Zone'
         ,field_type      = 'LONG'
         ,field_alias     = 'Zone'
      );
      
      arcpy.CalculateField_management(
          in_table        = output_name
         ,field           = 'Zone'
         ,expression      = 'int(str(!xxdzxx!))'
         ,expression_type = "PYTHON"
      );      
      
      arcpy.management.DeleteField(
          in_table        = output_name
         ,drop_field      = 'xxdzxx'
      );
      
   if has_line_id_fld and chk_fix_line_id:
      arcpy.management.AlterField(
          in_table        = output_name
         ,field           = fix_line_id_fld
         ,new_field_name  = 'xxdzxx'
      );
      
      arcpy.management.AlterField(
          in_table        = output_name
         ,field           = 'xxdzxx'
         ,new_field_name  = 'LINE_ID'
         ,new_field_alias = 'Line ID'
      );
      
   if not has_plume_area:
      arcpy.management.AddField(
          in_table     = output_name
         ,field_name   = 'Plumearea'
         ,field_type   = 'DOUBLE'
         ,field_alias  = 'Plume Area (SqM)'
      );
            
   if not has_zone_fld:
      arcpy.management.AddField(
          in_table     = output_name
         ,field_name   = 'Zone'
         ,field_type   = 'LONG'
         ,field_alias  = 'Zone'
      );

      with arcpy.da.UpdateCursor(
          in_table    = output_name
         ,field_names = ['Zone','Shape_Area']
         ,sql_clause  = (None, "ORDER BY Shape_Area ASC")
      ) as cursor:

         idx = 1;
         for row in cursor:
            if expected_zone is not None:
               row[0] = expected_zone;
            else:
               row[0] = idx;
            
            cursor.updateRow(row);
            idx += 1;
            
   if not has_line_id_fld:
      arcpy.management.AddField(
          in_table     = output_name
         ,field_name   = 'LINE_ID'
         ,field_type   = 'TEXT'
         ,field_length = 254
         ,field_alias  = 'Line ID'
      );

   return True;

