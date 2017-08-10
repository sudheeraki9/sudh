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
	    name = "IX_SPC_PERFORMANCEDataflow_Servlet",
	    urlPatterns = {
	      "/ix_spc_performance_df"})
public class IX_SPC_PERFORMANCEDataflow_Servlet extends HttpServlet implements Constants{
	static Logger log = Logger.getLogger(IX_SPC_PERFORMANCEDataflow_Servlet.class.getName());
	public void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws IOException {
	doPost(req,resp);
	}
	public void doPost(HttpServletRequest req, HttpServletResponse resp)
			throws IOException {
		
		try{

			String sampleSchema = "[{\"mode\": \"NULLABLE\",\"name\":\"DBTime\",\"type\":\"TIMESTAMP\",\"description\": \"DB time\"},{\"mode\": \"NULLABLE\",\"name\":\"DBUser\",\"type\":\"STRING\",\"description\": \"DB user\"},{\"mode\": \"NULLABLE\",\"name\":\"DBParentPlanogramKey\",\"type\":\"INTEGER\",\"description\": \"DB parent planogram key\"},{\"mode\": \"NULLABLE\",\"name\":\"DBParentProductKey\",\"type\":\"INTEGER\",\"description\": \"DB parent product key\"},{\"mode\": \"NULLABLE\",\"name\":\"DBKey\",\"type\":\"INTEGER\",\"description\": \"DBKey\"},{\"mode\": \"NULLABLE\",\"name\":\"Changed\",\"type\":\"INTEGER\",\"description\": \"Changed\"},{\"mode\": \"NULLABLE\",\"name\":\"Price\",\"type\":\"FLOAT\",\"description\": \"Price\"},{\"mode\": \"NULLABLE\",\"name\":\"CaseCost\",\"type\":\"FLOAT\",\"description\": \"Case cost\"},{\"mode\": \"NULLABLE\",\"name\":\"TaxCode\",\"type\":\"INTEGER\",\"description\": \"Tax code\"},{\"mode\": \"NULLABLE\",\"name\":\"UnitMovement\",\"type\":\"FLOAT\",\"description\": \"Unit movement\"},{\"mode\": \"NULLABLE\",\"name\":\"Share\",\"type\":\"FLOAT\",\"description\": \"Share\"},{\"mode\": \"NULLABLE\",\"name\":\"Facings\",\"type\":\"INTEGER\",\"description\": \"Facings\"},{\"mode\": \"NULLABLE\",\"name\":\"Units\",\"type\":\"INTEGER\",\"description\": \"Units\"},{\"mode\": \"NULLABLE\",\"name\":\"Capacity\",\"type\":\"INTEGER\",\"description\": \"Capacity\"},{\"mode\": \"NULLABLE\",\"name\":\"Linear\",\"type\":\"FLOAT\",\"description\": \"Linear\"},{\"mode\": \"NULLABLE\",\"name\":\"Square\",\"type\":\"FLOAT\",\"description\": \"Square\"},{\"mode\": \"NULLABLE\",\"name\":\"Cubic\",\"type\":\"FLOAT\",\"description\": \"Cubic\"},{\"mode\": \"NULLABLE\",\"name\":\"Sales\",\"type\":\"FLOAT\",\"description\": \"Sales\"},{\"mode\": \"NULLABLE\",\"name\":\"UnitCost\",\"type\":\"FLOAT\",\"description\": \"Unit cost\"},{\"mode\": \"NULLABLE\",\"name\":\"UnitProfit\",\"type\":\"FLOAT\",\"description\": \"Unit profit\"},{\"mode\": \"NULLABLE\",\"name\":\"Profit\",\"type\":\"FLOAT\",\"description\": \"Profit\"},{\"mode\": \"NULLABLE\",\"name\":\"SalesLinear\",\"type\":\"FLOAT\",\"description\": \"Sales/Linear\"},{\"mode\": \"NULLABLE\",\"name\":\"SalesSquare\",\"type\":\"FLOAT\",\"description\": \"Sales/Square\"},{\"mode\": \"NULLABLE\",\"name\":\"SalesCubic\",\"type\":\"FLOAT\",\"description\": \"Sales/Cubic\"},{\"mode\": \"NULLABLE\",\"name\":\"ProfitLinear\",\"type\":\"FLOAT\",\"description\": \"Profit/Linear\"},{\"mode\": \"NULLABLE\",\"name\":\"ProfitSquare\",\"type\":\"FLOAT\",\"description\": \"Profit/Square\"},{\"mode\": \"NULLABLE\",\"name\":\"ProfitCubic\",\"type\":\"FLOAT\",\"description\": \"Profit/Cubic\"},{\"mode\": \"NULLABLE\",\"name\":\"MovementLinear\",\"type\":\"FLOAT\",\"description\": \"Movement/Linear\"},{\"mode\": \"NULLABLE\",\"name\":\"MovementSquare\",\"type\":\"FLOAT\",\"description\": \"Movement/Square\"},{\"mode\": \"NULLABLE\",\"name\":\"MovementCubic\",\"type\":\"FLOAT\",\"description\": \"Movement/Cubic\"},{\"mode\": \"NULLABLE\",\"name\":\"Turns\",\"type\":\"FLOAT\",\"description\": \"Turns\"},{\"mode\": \"NULLABLE\",\"name\":\"MovementPrcnt\",\"type\":\"FLOAT\",\"description\": \"Movement%\"},{\"mode\": \"NULLABLE\",\"name\":\"ProfitPrcnt\",\"type\":\"FLOAT\",\"description\": \"Profit%\"},{\"mode\": \"NULLABLE\",\"name\":\"LinearPrcnt\",\"type\":\"FLOAT\",\"description\": \"Linear%\"},{\"mode\": \"NULLABLE\",\"name\":\"SquarePrcnt\",\"type\":\"FLOAT\",\"description\": \"Square%\"},{\"mode\": \"NULLABLE\",\"name\":\"CubicPrcnt\",\"type\":\"FLOAT\",\"description\": \"Cubic%\"},{\"mode\": \"NULLABLE\",\"name\":\"LinearPrcntUsed\",\"type\":\"FLOAT\",\"description\": \"Linear% used\"},{\"mode\": \"NULLABLE\",\"name\":\"SquarePrcntUsed\",\"type\":\"FLOAT\",\"description\": \"Square% used\"},{\"mode\": \"NULLABLE\",\"name\":\"CubicPrcntUsed\",\"type\":\"FLOAT\",\"description\": \"Cubic% used\"},{\"mode\": \"NULLABLE\",\"name\":\"Value1Prcnt\",\"type\":\"FLOAT\",\"description\": \"Value1%\"},{\"mode\": \"NULLABLE\",\"name\":\"Value2Prcnt\",\"type\":\"FLOAT\",\"description\": \"Value2%\"},{\"mode\": \"NULLABLE\",\"name\":\"Value3Prcnt\",\"type\":\"FLOAT\",\"description\": \"Value3%\"},{\"mode\": \"NULLABLE\",\"name\":\"ActualCaseMultiple\",\"type\":\"FLOAT\",\"description\": \"Actual case multiple\"},{\"mode\": \"NULLABLE\",\"name\":\"ActualDaysSupply\",\"type\":\"FLOAT\",\"description\": \"Actual days supply\"},{\"mode\": \"NULLABLE\",\"name\":\"TargetInventory\",\"type\":\"INTEGER\",\"description\": \"Target inventory\"},{\"mode\": \"NULLABLE\",\"name\":\"TargetInventoryMerch\",\"type\":\"INTEGER\",\"description\": \"Target inventory merch\"},{\"mode\": \"NULLABLE\",\"name\":\"ROIICost\",\"type\":\"FLOAT\",\"description\": \"ROII cost\"},{\"mode\": \"NULLABLE\",\"name\":\"ROIIRetail\",\"type\":\"FLOAT\",\"description\": \"ROII retail\"},{\"mode\": \"NULLABLE\",\"name\":\"CapacityCost\",\"type\":\"FLOAT\",\"description\": \"Capacity cost\"},{\"mode\": \"NULLABLE\",\"name\":\"CapacityRetail\",\"type\":\"FLOAT\",\"description\": \"Capacity retail\"},{\"mode\": \"NULLABLE\",\"name\":\"CostBeforeTax\",\"type\":\"FLOAT\",\"description\": \"Cost before tax\"},{\"mode\": \"NULLABLE\",\"name\":\"PriceBeforeTax\",\"type\":\"FLOAT\",\"description\": \"Price before tax\"},{\"mode\": \"NULLABLE\",\"name\":\"AnnualMovement\",\"type\":\"FLOAT\",\"description\": \"Annual movement\"},{\"mode\": \"NULLABLE\",\"name\":\"AnnualProfit\",\"type\":\"FLOAT\",\"description\": \"Annual profit\"},{\"mode\": \"NULLABLE\",\"name\":\"Overstock\",\"type\":\"INTEGER\",\"description\": \"Overstock\"},{\"mode\": \"NULLABLE\",\"name\":\"CombinedPerformanceIndex\",\"type\":\"FLOAT\",\"description\": \"Combined performance index\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc1\",\"type\":\"STRING\",\"description\": \"Desc  1\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc2\",\"type\":\"STRING\",\"description\": \"Desc  2\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc3\",\"type\":\"STRING\",\"description\": \"Desc  3\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc4\",\"type\":\"STRING\",\"description\": \"Desc  4\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc5\",\"type\":\"STRING\",\"description\": \"Desc  5\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc6\",\"type\":\"STRING\",\"description\": \"Desc  6\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc7\",\"type\":\"STRING\",\"description\": \"Desc  7\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc8\",\"type\":\"STRING\",\"description\": \"Desc  8\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc9\",\"type\":\"STRING\",\"description\": \"Desc  9\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc10\",\"type\":\"STRING\",\"description\": \"Desc 10\"},{\"mode\": \"NULLABLE\",\"name\":\"Value1\",\"type\":\"FLOAT\",\"description\": \"Value  1\"},{\"mode\": \"NULLABLE\",\"name\":\"Value2\",\"type\":\"FLOAT\",\"description\": \"Value  2\"},{\"mode\": \"NULLABLE\",\"name\":\"Value3\",\"type\":\"FLOAT\",\"description\": \"Value  3\"},{\"mode\": \"NULLABLE\",\"name\":\"Value4\",\"type\":\"FLOAT\",\"description\": \"Value  4\"},{\"mode\": \"NULLABLE\",\"name\":\"Value5\",\"type\":\"FLOAT\",\"description\": \"Value  5\"},{\"mode\": \"NULLABLE\",\"name\":\"Value6\",\"type\":\"FLOAT\",\"description\": \"Value  6\"},{\"mode\": \"NULLABLE\",\"name\":\"Value7\",\"type\":\"FLOAT\",\"description\": \"Value  7\"},{\"mode\": \"NULLABLE\",\"name\":\"Value8\",\"type\":\"FLOAT\",\"description\": \"Value  8\"},{\"mode\": \"NULLABLE\",\"name\":\"Value9\",\"type\":\"FLOAT\",\"description\": \"Value  9\"},{\"mode\": \"NULLABLE\",\"name\":\"Value10\",\"type\":\"FLOAT\",\"description\": \"Value 10\"},{\"mode\": \"NULLABLE\",\"name\":\"Flag1\",\"type\":\"INTEGER\",\"description\": \"Flag  1\"},{\"mode\": \"NULLABLE\",\"name\":\"Flag2\",\"type\":\"INTEGER\",\"description\": \"Flag  2\"},{\"mode\": \"NULLABLE\",\"name\":\"Flag3\",\"type\":\"INTEGER\",\"description\": \"Flag  3\"},{\"mode\": \"NULLABLE\",\"name\":\"Flag4\",\"type\":\"INTEGER\",\"description\": \"Flag  4\"},{\"mode\": \"NULLABLE\",\"name\":\"Flag5\",\"type\":\"INTEGER\",\"description\": \"Flag  5\"},{\"mode\": \"NULLABLE\",\"name\":\"Flag6\",\"type\":\"INTEGER\",\"description\": \"Flag  6\"},{\"mode\": \"NULLABLE\",\"name\":\"Flag7\",\"type\":\"INTEGER\",\"description\": \"Flag  7\"},{\"mode\": \"NULLABLE\",\"name\":\"Flag8\",\"type\":\"INTEGER\",\"description\": \"Flag  8\"},{\"mode\": \"NULLABLE\",\"name\":\"Flag9\",\"type\":\"INTEGER\",\"description\": \"Flag  9\"},{\"mode\": \"NULLABLE\",\"name\":\"Flag10\",\"type\":\"INTEGER\",\"description\": \"Flag 10\"},{\"mode\": \"NULLABLE\",\"name\":\"SalesPrcnt\",\"type\":\"FLOAT\",\"description\": \"Sales%\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc11\",\"type\":\"STRING\",\"description\": \"Desc 11\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc12\",\"type\":\"STRING\",\"description\": \"Desc 12\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc13\",\"type\":\"STRING\",\"description\": \"Desc 13\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc14\",\"type\":\"STRING\",\"description\": \"Desc 14\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc15\",\"type\":\"STRING\",\"description\": \"Desc 15\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc16\",\"type\":\"STRING\",\"description\": \"Desc 16\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc17\",\"type\":\"STRING\",\"description\": \"Desc 17\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc18\",\"type\":\"STRING\",\"description\": \"Desc 18\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc19\",\"type\":\"STRING\",\"description\": \"Desc 19\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc20\",\"type\":\"STRING\",\"description\": \"Desc 20\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc21\",\"type\":\"STRING\",\"description\": \"Desc 21\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc22\",\"type\":\"STRING\",\"description\": \"Desc 22\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc23\",\"type\":\"STRING\",\"description\": \"Desc 23\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc24\",\"type\":\"STRING\",\"description\": \"Desc 24\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc25\",\"type\":\"STRING\",\"description\": \"Desc 25\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc26\",\"type\":\"STRING\",\"description\": \"Desc 26\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc27\",\"type\":\"STRING\",\"description\": \"Desc 27\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc28\",\"type\":\"STRING\",\"description\": \"Desc 28\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc29\",\"type\":\"STRING\",\"description\": \"Desc 29\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc30\",\"type\":\"STRING\",\"description\": \"Desc 30\"},{\"mode\": \"NULLABLE\",\"name\":\"Value11\",\"type\":\"FLOAT\",\"description\": \"Value 11\"},{\"mode\": \"NULLABLE\",\"name\":\"Value12\",\"type\":\"FLOAT\",\"description\": \"Value 12\"},{\"mode\": \"NULLABLE\",\"name\":\"Value13\",\"type\":\"FLOAT\",\"description\": \"Value 13\"},{\"mode\": \"NULLABLE\",\"name\":\"Value14\",\"type\":\"FLOAT\",\"description\": \"Value 14\"},{\"mode\": \"NULLABLE\",\"name\":\"Value15\",\"type\":\"FLOAT\",\"description\": \"Value 15\"},{\"mode\": \"NULLABLE\",\"name\":\"Value16\",\"type\":\"FLOAT\",\"description\": \"Value 16\"},{\"mode\": \"NULLABLE\",\"name\":\"Value17\",\"type\":\"FLOAT\",\"description\": \"Value 17\"},{\"mode\": \"NULLABLE\",\"name\":\"Value18\",\"type\":\"FLOAT\",\"description\": \"Value 18\"},{\"mode\": \"NULLABLE\",\"name\":\"Value19\",\"type\":\"FLOAT\",\"description\": \"Value 19\"},{\"mode\": \"NULLABLE\",\"name\":\"Value20\",\"type\":\"FLOAT\",\"description\": \"Value 20\"},{\"mode\": \"NULLABLE\",\"name\":\"Value21\",\"type\":\"FLOAT\",\"description\": \"Value 21\"},{\"mode\": \"NULLABLE\",\"name\":\"Value22\",\"type\":\"FLOAT\",\"description\": \"Value 22\"},{\"mode\": \"NULLABLE\",\"name\":\"Value23\",\"type\":\"FLOAT\",\"description\": \"Value 23\"},{\"mode\": \"NULLABLE\",\"name\":\"Value24\",\"type\":\"FLOAT\",\"description\": \"Value 24\"},{\"mode\": \"NULLABLE\",\"name\":\"Value25\",\"type\":\"FLOAT\",\"description\": \"Value 25\"},{\"mode\": \"NULLABLE\",\"name\":\"Value26\",\"type\":\"FLOAT\",\"description\": \"Value 26\"},{\"mode\": \"NULLABLE\",\"name\":\"Value27\",\"type\":\"FLOAT\",\"description\": \"Value 27\"},{\"mode\": \"NULLABLE\",\"name\":\"Value28\",\"type\":\"FLOAT\",\"description\": \"Value 28\"},{\"mode\": \"NULLABLE\",\"name\":\"Value29\",\"type\":\"FLOAT\",\"description\": \"Value 29\"},{\"mode\": \"NULLABLE\",\"name\":\"Value30\",\"type\":\"FLOAT\",\"description\": \"Value 30\"},{\"mode\": \"NULLABLE\",\"name\":\"CaseMultiple\",\"type\":\"FLOAT\",\"description\": \"Case multiple\"},{\"mode\": \"NULLABLE\",\"name\":\"DaysSupply\",\"type\":\"FLOAT\",\"description\": \"Days supply\"},{\"mode\": \"NULLABLE\",\"name\":\"PeakSafetyFactor\",\"type\":\"FLOAT\",\"description\": \"Peak safety factor\"},{\"mode\": \"NULLABLE\",\"name\":\"BackroomStock\",\"type\":\"FLOAT\",\"description\": \"Backroom stock\"},{\"mode\": \"NULLABLE\",\"name\":\"MinimumUnits\",\"type\":\"INTEGER\",\"description\": \"Minimum units\"},{\"mode\": \"NULLABLE\",\"name\":\"MaximumUnits\",\"type\":\"INTEGER\",\"description\": \"Maximum units\"},{\"mode\": \"NULLABLE\",\"name\":\"DeliverySchedule\",\"type\":\"STRING\",\"description\": \"Delivery schedule\"},{\"mode\": \"NULLABLE\",\"name\":\"ReplenishmentMin\",\"type\":\"INTEGER\",\"description\": \"Replenishment min\"},{\"mode\": \"NULLABLE\",\"name\":\"ReplenishmentMax\",\"type\":\"INTEGER\",\"description\": \"Replenishment max\"},{\"mode\": \"NULLABLE\",\"name\":\"CPIRank\",\"type\":\"INTEGER\",\"description\": \"Assortment Rank\"},{\"mode\": \"NULLABLE\",\"name\":\"RecommendedFacings\",\"type\":\"INTEGER\",\"description\": \"Recommended facings\"},{\"mode\": \"NULLABLE\",\"name\":\"AssortmentStrategy\",\"type\":\"STRING\",\"description\": \"Assortment strategy\"},{\"mode\": \"NULLABLE\",\"name\":\"AssortmentTactic\",\"type\":\"STRING\",\"description\": \"Assortment tactic\"},{\"mode\": \"NULLABLE\",\"name\":\"AssortmentReason\",\"type\":\"STRING\",\"description\": \"Assortment reason\"},{\"mode\": \"NULLABLE\",\"name\":\"AssortmentAction\",\"type\":\"STRING\",\"description\": \"Assortment action\"},{\"mode\": \"NULLABLE\",\"name\":\"PartID\",\"type\":\"STRING\",\"description\": \"Part ID\"},{\"mode\": \"NULLABLE\",\"name\":\"NumberOfPositions\",\"type\":\"INTEGER\",\"description\": \"Number Of Positions\"},{\"mode\": \"NULLABLE\",\"name\":\"ClusterName\",\"type\":\"STRING\",\"description\": \"Cluster Name\"},{\"mode\": \"NULLABLE\",\"name\":\"TargetDistibutionStores\",\"type\":\"INTEGER\",\"description\": \"Target Distribution Stores\"},{\"mode\": \"NULLABLE\",\"name\":\"TargetDistibutionPrcnt\",\"type\":\"FLOAT\",\"description\": \"Target Distribution %\"},{\"mode\": \"NULLABLE\",\"name\":\"AssortmentNote\",\"type\":\"STRING\",\"description\": \"Assortment Note\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc31\",\"type\":\"STRING\",\"description\": \"Desc 31\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc32\",\"type\":\"STRING\",\"description\": \"Desc 32\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc33\",\"type\":\"STRING\",\"description\": \"Desc 33\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc34\",\"type\":\"STRING\",\"description\": \"Desc 34\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc35\",\"type\":\"STRING\",\"description\": \"Desc 35\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc36\",\"type\":\"STRING\",\"description\": \"Desc 36\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc37\",\"type\":\"STRING\",\"description\": \"Desc 38\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc38\",\"type\":\"STRING\",\"description\": \"Desc 38\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc39\",\"type\":\"STRING\",\"description\": \"Desc 39\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc40\",\"type\":\"STRING\",\"description\": \"Desc 40\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc41\",\"type\":\"STRING\",\"description\": \"Desc 41\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc42\",\"type\":\"STRING\",\"description\": \"Desc 42\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc43\",\"type\":\"STRING\",\"description\": \"Desc 43\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc44\",\"type\":\"STRING\",\"description\": \"Desc 44\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc45\",\"type\":\"STRING\",\"description\": \"Desc 45\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc46\",\"type\":\"STRING\",\"description\": \"Desc 46\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc47\",\"type\":\"STRING\",\"description\": \"Desc 47\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc48\",\"type\":\"STRING\",\"description\": \"Desc 48\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc49\",\"type\":\"STRING\",\"description\": \"Desc 49\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc50\",\"type\":\"STRING\",\"description\": \"Desc 50\"},{\"mode\": \"NULLABLE\",\"name\":\"Value31\",\"type\":\"FLOAT\",\"description\": \"Value 31\"},{\"mode\": \"NULLABLE\",\"name\":\"Value32\",\"type\":\"FLOAT\",\"description\": \"Value 32\"},{\"mode\": \"NULLABLE\",\"name\":\"Value33\",\"type\":\"FLOAT\",\"description\": \"Value 33\"},{\"mode\": \"NULLABLE\",\"name\":\"Value34\",\"type\":\"FLOAT\",\"description\": \"Value 34\"},{\"mode\": \"NULLABLE\",\"name\":\"Value35\",\"type\":\"FLOAT\",\"description\": \"Value 35\"},{\"mode\": \"NULLABLE\",\"name\":\"Value36\",\"type\":\"FLOAT\",\"description\": \"Value 36\"},{\"mode\": \"NULLABLE\",\"name\":\"Value37\",\"type\":\"FLOAT\",\"description\": \"Value 37\"},{\"mode\": \"NULLABLE\",\"name\":\"Value38\",\"type\":\"FLOAT\",\"description\": \"Value 38\"},{\"mode\": \"NULLABLE\",\"name\":\"Value39\",\"type\":\"FLOAT\",\"description\": \"Value 39\"},{\"mode\": \"NULLABLE\",\"name\":\"Value40\",\"type\":\"FLOAT\",\"description\": \"Value 40\"},{\"mode\": \"NULLABLE\",\"name\":\"Value41\",\"type\":\"FLOAT\",\"description\": \"Value 41\"},{\"mode\": \"NULLABLE\",\"name\":\"Value42\",\"type\":\"FLOAT\",\"description\": \"Value 42\"},{\"mode\": \"NULLABLE\",\"name\":\"Value43\",\"type\":\"FLOAT\",\"description\": \"Value 43\"},{\"mode\": \"NULLABLE\",\"name\":\"Value44\",\"type\":\"FLOAT\",\"description\": \"Value 44\"},{\"mode\": \"NULLABLE\",\"name\":\"Value45\",\"type\":\"FLOAT\",\"description\": \"Value 45\"},{\"mode\": \"NULLABLE\",\"name\":\"Value46\",\"type\":\"FLOAT\",\"description\": \"Value 46\"},{\"mode\": \"NULLABLE\",\"name\":\"Value47\",\"type\":\"FLOAT\",\"description\": \"Value 47\"},{\"mode\": \"NULLABLE\",\"name\":\"Value48\",\"type\":\"FLOAT\",\"description\": \"Value 48\"},{\"mode\": \"NULLABLE\",\"name\":\"Value49\",\"type\":\"FLOAT\",\"description\": \"Value 49\"},{\"mode\": \"NULLABLE\",\"name\":\"Value50\",\"type\":\"FLOAT\",\"description\": \"Value 50\"},{\"mode\": \"NULLABLE\",\"name\":\"CapacityUnrestricted\",\"type\":\"INTEGER\",\"description\": \"Capacity Unrestricted\"},{\"mode\": \"NULLABLE\",\"name\":\"CustomData\",\"type\":\"STRING\",\"description\": \"Custom Data\"},{\"mode\": \"NULLABLE\",\"name\":\"RecommendedOrientation\",\"type\":\"INTEGER\",\"description\": \"Recommended Orientation\"},{\"mode\": \"NULLABLE\",\"name\":\"RecommendedMerchStyle\",\"type\":\"INTEGER\",\"description\": \"Recommended Merch Style\"},{\"mode\": \"NULLABLE\",\"name\":\"IgnoreRecommendations\",\"type\":\"INTEGER\",\"description\": \"Ignore Recommendations\"},{\"mode\": \"NULLABLE\",\"name\":\"Priority\",\"type\":\"INTEGER\",\"description\": \"Priority\"},{\"mode\": \"NULLABLE\",\"name\":\"PriorityDesc\",\"type\":\"STRING\",\"description\": \"Priority Desc\"},{\"mode\": \"NULLABLE\",\"name\":\"ForceList\",\"type\":\"INTEGER\",\"description\": \"Force List\"},{\"mode\": \"NULLABLE\",\"name\":\"PGReasons\",\"type\":\"STRING\",\"description\": \"PG Reasons\"},{\"mode\": \"NULLABLE\",\"name\":\"PGMaxStageReduce\",\"type\":\"FLOAT\",\"description\": \"PG Max Stage Reduce\"},{\"mode\": \"NULLABLE\",\"name\":\"PGMaxStageFillOut\",\"type\":\"FLOAT\",\"description\": \"PG Max Stage Fill Out\"},{\"mode\": \"NULLABLE\",\"name\":\"RecommendedChange\",\"type\":\"INTEGER\",\"description\": \"Recommended Change\"},{\"mode\": \"NULLABLE\",\"name\":\"FILE_NAME_DATE\",\"type\":\"STRING\",\"description\": \"file name date\"}]";
			
			 List<String> fileNameList = StorageService.readFileFromStorageFolder(BUCKETNAME, "IX_SPC_PERFORMANCE_", "performanceProcess");
			 log.info("Total files" + fileNameList.size());
			 if(fileNameList.size() == 0){		 
				
					log.info("No Files found");
					MailUser.sendMail(PROJECT_NAME + " : DUP_IX_SPC_PERFORMANCE Notification","No Files found after unzipping"); 
			 }
			 
			 for(int i =0 ; i<fileNameList.size(); i++ ){
					
					
					String dataformat = fileNameList.get(i).replace("IX_SPC_PERFORMANCE_", "");
					dataformat = dataformat.substring(0, 8);
					
					String tableSpec = PROJECT_NAME + ":JDA_Tables.IX_SPC_PERFORMANCE";
					String tableErrorSpec  = PROJECT_NAME+":JDA_Error.IX_SPC_PERFORMANCE_ErrorLog";
					
					
					String successtable = tableSpec + "_" + dataformat;
					String errorTable = tableErrorSpec + "_" + dataformat;

					String filepath = "gs://"+ BUCKETNAME   + "/performanceProcess/" + fileNameList.get(i);
					StorageService.deletingFileFromStorage(BUCKETNAME,  "IX_SPC_PERFORMANCE_"+dataformat);
					//StorageService.copyingFilefromPath(BUCKETNAME,BUCKETNAME_ARCHIVE,  fileNameList.get(i), "");
					DataFlowProcessForJDA.run(sampleSchema,successtable,errorTable,TEMPLOCATION,filepath,"","DBTime",DelimiterType.PIPE,"IX_SPC_PERFORMANCE_"+dataformat);		
					DeleteErrorTable.deleteErrorTable("IX_SPC_PERFORMANCE_ErrorLog_" + dataformat, "JDA_Error",errorTable, "IX_SPC_PERFORMANCE");	
					DeleteErrorTable.successTableNotification("IX_SPC_PERFORMANCE_"+ dataformat, "JDA_Tables", successtable, "IX_SPC_PERFORMANCE");					
					StorageService.deletingFileFromStorageFromProcessing(BUCKETNAME, fileNameList.get(i), "performanceProcess");
					log.info("Dataflow completed for Product");
				}
			}
			catch(Exception e){
				log.info("Exception Occured : " + e.getLocalizedMessage());
				MailUser.sendMail(PROJECT_NAME + " : DUP_IX_SPC_PERFORMANCE Notification","Job failed because of below reason : " + e.getMessage()); 
			}
		}
	}		