package com.servlet;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.process.DataFlowProcessForJDA;
import com.process.DeleteErrorTable;
import com.process.MailUser;
import com.process.StorageService;
import com.utils.Constants;
import com.utils.SchemaDef.DelimiterType;

@SuppressWarnings("serial")
@WebServlet(
	    name = "IX_SPC_SEGMENT_Servlet",
	    urlPatterns = {
	      "/ix_spc_segment"})
public class IX_SPC_SEGMENT_Servlet extends HttpServlet implements Constants{
	static Logger log = Logger.getLogger(IX_SPC_SEGMENT_Servlet.class.getName());
	public void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws IOException {
		try{

			String sampleSchema = "[{\"mode\": \"NULLABLE\",\"name\":\"DBTime\",\"type\":\"TIMESTAMP\",\"description\":\"DB time \"},{\"mode\": \"NULLABLE\",\"name\":\"DBUser\",\"type\":\"STRING\",\"description\":\"DB user \"},{\"mode\": \"NULLABLE\",\"name\":\"DBParentPlanogramKey\",\"type\":\"INTEGER\",\"description\":\"DB parent planogram key \"},{\"mode\": \"NULLABLE\",\"name\":\"Name\",\"type\":\"STRING\",\"description\":\"Name \"},{\"mode\": \"NULLABLE\",\"name\":\"DBKey\",\"type\":\"INTEGER\",\"description\":\"DBKey \"},{\"mode\": \"NULLABLE\",\"name\":\"X\",\"type\":\"FLOAT\",\"description\":\"X \"},{\"mode\": \"NULLABLE\",\"name\":\"Width\",\"type\":\"FLOAT\",\"description\":\"Width \"},{\"mode\": \"NULLABLE\",\"name\":\"Y\",\"type\":\"FLOAT\",\"description\":\"Y \"},{\"mode\": \"NULLABLE\",\"name\":\"Height\",\"type\":\"FLOAT\",\"description\":\"Height \"},{\"mode\": \"NULLABLE\",\"name\":\"Z\",\"type\":\"FLOAT\",\"description\":\"Z \"},{\"mode\": \"NULLABLE\",\"name\":\"Depth\",\"type\":\"FLOAT\",\"description\":\"Depth \"},{\"mode\": \"NULLABLE\",\"name\":\"Angle\",\"type\":\"FLOAT\",\"description\":\"Angle \"},{\"mode\": \"NULLABLE\",\"name\":\"Changed\",\"type\":\"INTEGER\",\"description\":\"Changed \"},{\"mode\": \"NULLABLE\",\"name\":\"OffsetX\",\"type\":\"FLOAT\",\"description\":\"Offset X \"},{\"mode\": \"NULLABLE\",\"name\":\"OffsetY\",\"type\":\"FLOAT\",\"description\":\"Offset Y \"},{\"mode\": \"NULLABLE\",\"name\":\"Door\",\"type\":\"INTEGER\",\"description\":\"Door \"},{\"mode\": \"NULLABLE\",\"name\":\"DoorDirection\",\"type\":\"INTEGER\",\"description\":\"Door direction \"},{\"mode\": \"NULLABLE\",\"name\":\"Desc1\",\"type\":\"STRING\",\"description\":\"Desc  1 \"},{\"mode\": \"NULLABLE\",\"name\":\"Desc2\",\"type\":\"STRING\",\"description\":\"Desc  2 \"},{\"mode\": \"NULLABLE\",\"name\":\"Desc3\",\"type\":\"STRING\",\"description\":\"Desc  3 \"},{\"mode\": \"NULLABLE\",\"name\":\"Desc4\",\"type\":\"STRING\",\"description\":\"Desc  4 \"},{\"mode\": \"NULLABLE\",\"name\":\"Desc5\",\"type\":\"STRING\",\"description\":\"Desc  5 \"},{\"mode\": \"NULLABLE\",\"name\":\"Desc6\",\"type\":\"STRING\",\"description\":\"Desc  6 \"},{\"mode\": \"NULLABLE\",\"name\":\"Desc7\",\"type\":\"STRING\",\"description\":\"Desc  7 \"},{\"mode\": \"NULLABLE\",\"name\":\"Desc8\",\"type\":\"STRING\",\"description\":\"Desc  8 \"},{\"mode\": \"NULLABLE\",\"name\":\"Desc9\",\"type\":\"STRING\",\"description\":\"Desc  9 \"},{\"mode\": \"NULLABLE\",\"name\":\"Desc10\",\"type\":\"STRING\",\"description\":\"Desc 10 \"},{\"mode\": \"NULLABLE\",\"name\":\"Value1\",\"type\":\"FLOAT\",\"description\":\"Value  1 \"},{\"mode\": \"NULLABLE\",\"name\":\"Value2\",\"type\":\"FLOAT\",\"description\":\"Value  2 \"},{\"mode\": \"NULLABLE\",\"name\":\"Value3\",\"type\":\"FLOAT\",\"description\":\"Value  3 \"},{\"mode\": \"NULLABLE\",\"name\":\"Value4\",\"type\":\"FLOAT\",\"description\":\"Value  4 \"},{\"mode\": \"NULLABLE\",\"name\":\"Value5\",\"type\":\"FLOAT\",\"description\":\"Value  5 \"},{\"mode\": \"NULLABLE\",\"name\":\"Value6\",\"type\":\"FLOAT\",\"description\":\"Value  6 \"},{\"mode\": \"NULLABLE\",\"name\":\"Value7\",\"type\":\"FLOAT\",\"description\":\"Value  7 \"},{\"mode\": \"NULLABLE\",\"name\":\"Value8\",\"type\":\"FLOAT\",\"description\":\"Value  8 \"},{\"mode\": \"NULLABLE\",\"name\":\"Value9\",\"type\":\"FLOAT\",\"description\":\"Value  9 \"},{\"mode\": \"NULLABLE\",\"name\":\"Value10\",\"type\":\"FLOAT\",\"description\":\"Value 10 \"},{\"mode\": \"NULLABLE\",\"name\":\"Flag1\",\"type\":\"INTEGER\",\"description\":\"Flag  1 \"},{\"mode\": \"NULLABLE\",\"name\":\"Flag2\",\"type\":\"INTEGER\",\"description\":\"Flag  2 \"},{\"mode\": \"NULLABLE\",\"name\":\"Flag3\",\"type\":\"INTEGER\",\"description\":\"Flag  3 \"},{\"mode\": \"NULLABLE\",\"name\":\"Flag4\",\"type\":\"INTEGER\",\"description\":\"Flag  4 \"},{\"mode\": \"NULLABLE\",\"name\":\"Flag5\",\"type\":\"INTEGER\",\"description\":\"Flag  5 \"},{\"mode\": \"NULLABLE\",\"name\":\"Flag6\",\"type\":\"INTEGER\",\"description\":\"Flag  6 \"},{\"mode\": \"NULLABLE\",\"name\":\"Flag7\",\"type\":\"INTEGER\",\"description\":\"Flag  7 \"},{\"mode\": \"NULLABLE\",\"name\":\"Flag8\",\"type\":\"INTEGER\",\"description\":\"Flag  8 \"},{\"mode\": \"NULLABLE\",\"name\":\"Flag9\",\"type\":\"INTEGER\",\"description\":\"Flag  9 \"},{\"mode\": \"NULLABLE\",\"name\":\"Flag10\",\"type\":\"INTEGER\",\"description\":\"Flag 10 \"},{\"mode\": \"NULLABLE\",\"name\":\"Number\",\"type\":\"INTEGER\",\"description\":\"Number \"},{\"mode\": \"NULLABLE\",\"name\":\"FrameWidth\",\"type\":\"FLOAT\",\"description\":\"Frame width \"},{\"mode\": \"NULLABLE\",\"name\":\"FrameHeight\",\"type\":\"FLOAT\",\"description\":\"Frame height \"},{\"mode\": \"NULLABLE\",\"name\":\"FrameColor\",\"type\":\"INTEGER\",\"description\":\"Frame color \"},{\"mode\": \"NULLABLE\",\"name\":\"FrameFillPattern\",\"type\":\"INTEGER\",\"description\":\"Frame fill pattern \"},{\"mode\": \"NULLABLE\",\"name\":\"PartID\",\"type\":\"STRING\",\"description\":\"Part ID \"},{\"mode\": \"NULLABLE\",\"name\":\"GLN\",\"type\":\"STRING\",\"description\":\"GLN \"},{\"mode\": \"NULLABLE\",\"name\":\"Warning\",\"type\":\"STRING\",\"description\":\"Warning \"},{\"mode\": \"NULLABLE\",\"name\":\"WarningNumber\",\"type\":\"INTEGER\",\"description\":\"Warning Number \"},{\"mode\": \"NULLABLE\",\"name\":\"CustomData\",\"type\":\"STRING\",\"description\":\"Custom Data \"},{\"mode\": \"NULLABLE\",\"name\":\"CanSeparate\",\"type\":\"INTEGER\",\"description\":\"Can Separate \"},{\"mode\": \"NULLABLE\",\"name\":\"NumberOfFixtures\",\"type\":\"INTEGER\",\"description\":\"Number Of Fixtures \"},{\"mode\": \"NULLABLE\",\"name\":\"NumberOfProductsAllocated\",\"type\":\"INTEGER\",\"description\":\"Number Of Products Allocated \"},{\"mode\": \"NULLABLE\",\"name\":\"Capacity\",\"type\":\"INTEGER\",\"description\":\"Capacity \"},{\"mode\": \"NULLABLE\",\"name\":\"CapacityCost\",\"type\":\"FLOAT\",\"description\":\"Capacity Cost \"},{\"mode\": \"NULLABLE\",\"name\":\"CapacityRetail\",\"type\":\"FLOAT\",\"description\":\"Capacity Retail \"},{\"mode\": \"NULLABLE\",\"name\":\"CapacityUnrestricted\",\"type\":\"INTEGER\",\"description\":\"Capacity Unrestricted \"},{\"mode\": \"NULLABLE\",\"name\":\"MovementAllocated\",\"type\":\"FLOAT\",\"description\":\"Movement Allocated \"},{\"mode\": \"NULLABLE\",\"name\":\"SalesAllocated\",\"type\":\"FLOAT\",\"description\":\"Sales Allocated \"},{\"mode\": \"NULLABLE\",\"name\":\"CostAllocated\",\"type\":\"FLOAT\",\"description\":\"Cost Allocated \"},{\"mode\": \"NULLABLE\",\"name\":\"MarginAllocated\",\"type\":\"FLOAT\",\"description\":\"Margin Allocated \"},{\"mode\": \"NULLABLE\",\"name\":\"ProfitAllocated\",\"type\":\"FLOAT\",\"description\":\"Profit Allocated \"},{\"mode\": \"NULLABLE\",\"name\":\"AnnualProfitAllocated\",\"type\":\"FLOAT\",\"description\":\"Annual Profit Allocated \"},{\"mode\": \"NULLABLE\",\"name\":\"ROIICostAllocated\",\"type\":\"FLOAT\",\"description\":\"ROII Cost Allocated \"},{\"mode\": \"NULLABLE\",\"name\":\"ROIIRetailAllocated\",\"type\":\"FLOAT\",\"description\":\"ROII Retail Allocated \"},{\"mode\": \"NULLABLE\",\"name\":\"CombinedPerfIndexAllocated\",\"type\":\"FLOAT\",\"description\":\"Combined PerfIndex Allocated \"},{\"mode\": \"NULLABLE\",\"name\":\"FILE_NAME_DATE\",\"type\":\"STRING\",\"description\":\"file name date\"}]";
			
			 List<String> fileNameList = StorageService.readFileFromStorage(BUCKETNAME, "IX_SPC_SEGMENT_", "");
			 log.info("Total files" + fileNameList.size());
			 if(fileNameList.size() == 0){		 
				
					log.info("No Files found");
					MailUser.sendMail(PROJECT_NAME + " : DUP_IX_SPC_SEGMENT Notification", CONTENT); 
			 }
			 
			 for(int i =0 ; i<fileNameList.size(); i++ ){
					
					
					String dataformat = fileNameList.get(i).replace("IX_SPC_SEGMENT_", "");
					dataformat = dataformat.replace(".csv.gz", "");
					
					String tableSpec = PROJECT_NAME + ":JDA_Tables.IX_SPC_SEGMENT";
					String tableErrorSpec  = PROJECT_NAME+":JDA_Error.IX_SPC_SEGMENT_ErrorLog";
					
					String successtable = tableSpec + "_" + dataformat;
					String errorTable = tableErrorSpec + "_" + dataformat;

					String filepath = "gs://"+ BUCKETNAME   + "/" + fileNameList.get(i);

					StorageService.copyingFilefromPath(BUCKETNAME,BUCKETNAME_ARCHIVE,  fileNameList.get(i), "");
					DataFlowProcessForJDA.run(sampleSchema,successtable,errorTable,TEMPLOCATION,filepath,"","DBTime",DelimiterType.PIPE,fileNameList.get(i));		
					DeleteErrorTable.deleteErrorTable("IX_SPC_SEGMENT_ErrorLog_" + dataformat, "JDA_Error",errorTable, "IX_SPC_SEGMENT");	
					DeleteErrorTable.successTableNotification("IX_SPC_SEGMENT_"+ dataformat, "JDA_Tables", successtable, "IX_SPC_SEGMENT");					
					StorageService.deletingFileFromStorage(BUCKETNAME, fileNameList.get(i));
					log.info("Dataflow completed for Product");
				}
			}
			catch(Exception e){
				log.info("Exception Occured : " + e.getLocalizedMessage());
				MailUser.sendMail(PROJECT_NAME + " : DUP_IX_SPC_SEGMENT Notification","Job failed because of below reason : " + e.getMessage()); 
			}
		}
	}
			
