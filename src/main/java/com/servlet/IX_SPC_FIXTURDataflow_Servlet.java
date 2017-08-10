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
	    name = "IX_SPC_FIXTURDataflow_Servlet",
	    urlPatterns = {
	      "/ix_spc_fixture_df"})
public class IX_SPC_FIXTURDataflow_Servlet extends HttpServlet implements Constants{
	static Logger log = Logger.getLogger(IX_SPC_FIXTURDataflow_Servlet.class.getName());
	
	public void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws IOException {
	
		try{

		String sampleSchema = "[{\"mode\": \"NULLABLE\",\"name\":\"DBTime\",\"type\":\"TIMESTAMP\",\"description\": \"DB time\"},{\"mode\": \"NULLABLE\",\"name\":\"DBUser\",\"type\":\"STRING\",\"description\": \"DB user\"},{\"mode\": \"NULLABLE\",\"name\":\"DBParentPlanogramKey\",\"type\":\"INTEGER\",\"description\": \"DB parent planogram key\"},{\"mode\": \"NULLABLE\",\"name\":\"Type\",\"type\":\"INTEGER\",\"description\": \"Type\"},{\"mode\": \"NULLABLE\",\"name\":\"Name\",\"type\":\"STRING\",\"description\": \"Name\"},{\"mode\": \"NULLABLE\",\"name\":\"DBKey\",\"type\":\"INTEGER\",\"description\":\"DBKey\"},{\"mode\": \"NULLABLE\",\"name\":\"X\",\"type\":\"FLOAT\",\"description\": \"X\"},{\"mode\": \"NULLABLE\",\"name\":\"Width\",\"type\":\"FLOAT\",\"description\":  \"Width\"},{\"mode\": \"NULLABLE\",\"name\":\"Y\",\"type\":\"FLOAT\",\"description\":  \"Y\"},{\"mode\": \"NULLABLE\",\"name\":\"Height\",\"type\":\"FLOAT\",\"description\": \"Height\"},{\"mode\": \"NULLABLE\",\"name\":\"Z\",\"type\":\"FLOAT\",\"description\":  \"Z\"},{\"mode\": \"NULLABLE\",\"name\":\"Depth\",\"type\":\"FLOAT\",\"description\": \"Depth\"},{\"mode\": \"NULLABLE\",\"name\":\"Slope\",\"type\":\"FLOAT\",\"description\": \"Slope\"},{\"mode\": \"NULLABLE\",\"name\":\"Angle\",\"type\":\"FLOAT\",\"description\": \"Angle\"},{\"mode\": \"NULLABLE\",\"name\":\"Roll\",\"type\":\"FLOAT\",\"description\":  \"Roll\"},{\"mode\": \"NULLABLE\",\"name\":\"Color\",\"type\":\"INTEGER\",\"description\": \"Color\"},{\"mode\": \"NULLABLE\",\"name\":\"Changed\",\"type\":\"INTEGER\",\"description\": \"Changed\"},{\"mode\": \"NULLABLE\",\"name\":\"Assembly\",\"type\":\"STRING\",\"description\":\"Assembly\"},{\"mode\": \"NULLABLE\",\"name\":\"XSpacing\",\"type\":\"FLOAT\",\"description\": \"X spacing\"},{\"mode\": \"NULLABLE\",\"name\":\"YSpacing\",\"type\":\"FLOAT\",\"description\": \"Y spacing\"},{\"mode\": \"NULLABLE\",\"name\":\"XStart\",\"type\":\"FLOAT\",\"description\": \"X start\"},{\"mode\": \"NULLABLE\",\"name\":\"YStart\",\"type\":\"FLOAT\",\"description\": \"Y start\"},{\"mode\": \"NULLABLE\",\"name\":\"WallWidth\",\"type\":\"FLOAT\",\"description\": \"Wall width\"},{\"mode\": \"NULLABLE\",\"name\":\"WallHeight\",\"type\":\"FLOAT\",\"description\":\"Wall height\"},{\"mode\": \"NULLABLE\",\"name\":\"WallDepth\",\"type\":\"FLOAT\",\"description\": \"Wall depth\"},{\"mode\": \"NULLABLE\",\"name\":\"Curve\",\"type\":\"FLOAT\",\"description\": \"Curve\"},{\"mode\": \"NULLABLE\",\"name\":\"Merch\",\"type\":\"FLOAT\",\"description\": \"Merch\"},{\"mode\": \"NULLABLE\",\"name\":\"CheckOtherFixtures\",\"type\":\"INTEGER\",\"description\": \"Check other fixtures\"},{\"mode\": \"NULLABLE\",\"name\":\"CheckOtherPositions\",\"type\":\"INTEGER\",\"description\": \"Check other positions\"},{\"mode\": \"NULLABLE\",\"name\":\"CanObstruct\",\"type\":\"INTEGER\",\"description\": \"Can obstruct\"},{\"mode\": \"NULLABLE\",\"name\":\"LeftOverhang\",\"type\":\"FLOAT\",\"description\":  \"Left overhang\"},{\"mode\": \"NULLABLE\",\"name\":\"RightOverhang\",\"type\":\"FLOAT\",\"description\": \"Right overhang\"},{\"mode\": \"NULLABLE\",\"name\":\"LowerOverhang\",\"type\":\"FLOAT\",\"description\": \"Lower overhang\"},{\"mode\": \"NULLABLE\",\"name\":\"UpperOverhang\",\"type\":\"FLOAT\",\"description\": \"Upper overhang\"},{\"mode\": \"NULLABLE\",\"name\":\"BackOverhang\",\"type\":\"FLOAT\",\"description\":  \"Back overhang\"},{\"mode\": \"NULLABLE\",\"name\":\"FrontOverhang\",\"type\":\"FLOAT\",\"description\": \"Front overhang\"},{\"mode\": \"NULLABLE\",\"name\":\"DefaultMerchStyle\",\"type\":\"INTEGER\",\"description\": \"Default merch style\"},{\"mode\": \"NULLABLE\",\"name\":\"DividerWidth\",\"type\":\"FLOAT\",\"description\": \"Divider width\"},{\"mode\": \"NULLABLE\",\"name\":\"DividerHeight\",\"type\":\"FLOAT\",\"description\":\"Divider height\"},{\"mode\": \"NULLABLE\",\"name\":\"DividerDepth\",\"type\":\"FLOAT\",\"description\": \"Divider depth\"},{\"mode\": \"NULLABLE\",\"name\":\"CanCombine\",\"type\":\"INTEGER\",\"description\": \"Can combine\"},{\"mode\": \"NULLABLE\",\"name\":\"GrilleHeight\",\"type\":\"FLOAT\",\"description\": \"Grille Height\"},{\"mode\": \"NULLABLE\",\"name\":\"NotchOffset\",\"type\":\"FLOAT\",\"description\":  \"Notch Offset\"},{\"mode\": \"NULLABLE\",\"name\":\"XSpacing2\",\"type\":\"FLOAT\",\"description\": \"X spacing 2\"},{\"mode\": \"NULLABLE\",\"name\":\"XStart2\",\"type\":\"FLOAT\",\"description\": \"X start 2\"},{\"mode\": \"NULLABLE\",\"name\":\"PegDrop\",\"type\":\"FLOAT\",\"description\": \"Peg drop\"},{\"mode\": \"NULLABLE\",\"name\":\"PegGapX\",\"type\":\"FLOAT\",\"description\": \"Peg gap X\"},{\"mode\": \"NULLABLE\",\"name\":\"PegGapY\",\"type\":\"FLOAT\",\"description\": \"Peg gap Y\"},{\"mode\": \"NULLABLE\",\"name\":\"PrimaryLabelFormatName\",\"type\":\"STRING\",\"description\": \"Primary fixture label format name\"},{\"mode\": \"NULLABLE\",\"name\":\"SecondaryLabelFormatName\",\"type\":\"STRING\",\"description\": \"Secondary fixture label format name\"},{\"mode\": \"NULLABLE\",\"name\":\"ShapeID\",\"type\":\"STRING\",\"description\": \"Shape ID\"},{\"mode\": \"NULLABLE\",\"name\":\"BitmapID\",\"type\":\"STRING\",\"description\": \"Bitmap ID\"},{\"mode\": \"NULLABLE\",\"name\":\"MerchXMin\",\"type\":\"INTEGER\",\"description\": \"MerchXMin\"},{\"mode\": \"NULLABLE\",\"name\":\"MerchXMax\",\"type\":\"INTEGER\",\"description\": \"MerchXMax\"},{\"mode\": \"NULLABLE\",\"name\":\"MerchXUprights\",\"type\":\"INTEGER\",\"description\":\"MerchXUprights\"},{\"mode\": \"NULLABLE\",\"name\":\"MerchXCaps\",\"type\":\"INTEGER\",\"description\": \"MerchXCaps\"},{\"mode\": \"NULLABLE\",\"name\":\"MerchXPlacement\",\"type\":\"INTEGER\",\"description\": \"MerchXPlacement\"},{\"mode\": \"NULLABLE\",\"name\":\"MerchXNumber\",\"type\":\"INTEGER\",\"description\":\"MerchXNumber\"},{\"mode\": \"NULLABLE\",\"name\":\"MerchXSize\",\"type\":\"INTEGER\",\"description\": \"MerchXSize\"},{\"mode\": \"NULLABLE\",\"name\":\"MerchXDirection\",\"type\":\"INTEGER\",\"description\": \"MerchXDirection\"},{\"mode\": \"NULLABLE\",\"name\":\"MerchXSqueeze\",\"type\":\"INTEGER\",\"description\": \"MerchXSqueeze\"},{\"mode\": \"NULLABLE\",\"name\":\"MerchYMin\",\"type\":\"INTEGER\",\"description\": \"MerchYMin\"},{\"mode\": \"NULLABLE\",\"name\":\"MerchYMax\",\"type\":\"INTEGER\",\"description\":  \"MerchYMax\"},{\"mode\": \"NULLABLE\",\"name\":\"MerchYUprights\",\"type\":\"INTEGER\",\"description\": \"MerchYUprights\"},{\"mode\": \"NULLABLE\",\"name\":\"MerchYCaps\",\"type\":\"INTEGER\",\"description\": \"MerchYCaps\"},{\"mode\": \"NULLABLE\",\"name\":\"MerchYPlacement\",\"type\":\"INTEGER\",\"description\": \"MerchYPlacement\"},{\"mode\": \"NULLABLE\",\"name\":\"MerchYNumber\",\"type\":\"INTEGER\",\"description\":  \"MerchYNumber\"},{\"mode\": \"NULLABLE\",\"name\":\"MerchYSize\",\"type\":\"INTEGER\",\"description\":  \"MerchYSize\"},{\"mode\": \"NULLABLE\",\"name\":\"MerchYDirection\",\"type\":\"INTEGER\",\"description\": \"MerchYDirection\"},{\"mode\": \"NULLABLE\",\"name\":\"MerchYSqueeze\",\"type\":\"INTEGER\",\"description\": \"MerchYSqueeze\"},{\"mode\": \"NULLABLE\",\"name\":\"MerchZMin\",\"type\":\"INTEGER\",\"description\": \"MerchZMin\"},{\"mode\": \"NULLABLE\",\"name\":\"MerchZMax\",\"type\":\"INTEGER\",\"description\": \"MerchZMax\"},{\"mode\": \"NULLABLE\",\"name\":\"MerchZUprights\",\"type\":\"INTEGER\",\"description\": \"MerchZUprights\"},{\"mode\": \"NULLABLE\",\"name\":\"MerchZCaps\",\"type\":\"INTEGER\",\"description\": \"MerchZCaps\"},{\"mode\": \"NULLABLE\",\"name\":\"MerchZPlacement\",\"type\":\"INTEGER\",\"description\": \"MerchZPlacement\"},{\"mode\": \"NULLABLE\",\"name\":\"MerchZNumber\",\"type\":\"INTEGER\",\"description\": \"MerchZNumber\"},{\"mode\": \"NULLABLE\",\"name\":\"MerchZSize\",\"type\":\"INTEGER\",\"description\": \"MerchZSize\"},{\"mode\": \"NULLABLE\",\"name\":\"MerchZDirection\",\"type\":\"INTEGER\",\"description\": \"MerchZDirection\"},{\"mode\": \"NULLABLE\",\"name\":\"MerchZSqueeze\",\"type\":\"INTEGER\",\"description\": \"MerchZSqueeze\"},{\"mode\": \"NULLABLE\",\"name\":\"NumberOfPositions\",\"type\":\"INTEGER\",\"description\":  \"Number of Positions\"},{\"mode\": \"NULLABLE\",\"name\":\"NumberOfDividers\",\"type\":\"INTEGER\",\"description\": \"Number of Dividers\"},{\"mode\": \"NULLABLE\",\"name\":\"Linear\",\"type\":\"FLOAT\",\"description\": \"Linear\"},{\"mode\": \"NULLABLE\",\"name\":\"Square\",\"type\":\"FLOAT\",\"description\": \"Square\"},{\"mode\": \"NULLABLE\",\"name\":\"Cubic\",\"type\":\"FLOAT\",\"description\": \"Cubic\"},{\"mode\": \"NULLABLE\",\"name\":\"Columns\",\"type\":\"INTEGER\",\"description\": \"Columns\"},{\"mode\": \"NULLABLE\",\"name\":\"Rows\",\"type\":\"INTEGER\",\"description\": \"Rows\"},{\"mode\": \"NULLABLE\",\"name\":\"Warning\",\"type\":\"STRING\",\"description\": \"Warning\"},{\"mode\": \"NULLABLE\",\"name\":\"WarningNumber\",\"type\":\"INTEGER\",\"description\": \"Warning number\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc1\",\"type\":\"STRING\",\"description\": \"Desc  1\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc2\",\"type\":\"STRING\",\"description\": \"Desc  2\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc3\",\"type\":\"STRING\",\"description\": \"Desc  3\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc4\",\"type\":\"STRING\",\"description\": \"Desc  4\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc5\",\"type\":\"STRING\",\"description\": \"Desc  5\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc6\",\"type\":\"STRING\",\"description\": \"Desc  6\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc7\",\"type\":\"STRING\",\"description\": \"Desc  7\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc8\",\"type\":\"STRING\",\"description\": \"Desc  8\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc9\",\"type\":\"STRING\",\"description\": \"Desc  9\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc10\",\"type\":\"STRING\",\"description\":\"Desc 10\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc11\",\"type\":\"STRING\",\"description\":\"Desc 11\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc12\",\"type\":\"STRING\",\"description\":\"Desc 12\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc13\",\"type\":\"STRING\",\"description\":\"Desc 13\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc14\",\"type\":\"STRING\",\"description\":\"Desc 14\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc15\",\"type\":\"STRING\",\"description\":\"Desc 15\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc16\",\"type\":\"STRING\",\"description\":\"Desc 16\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc17\",\"type\":\"STRING\",\"description\":\"Desc 17\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc18\",\"type\":\"STRING\",\"description\":\"Desc 18\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc19\",\"type\":\"STRING\",\"description\":\"Desc 19\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc20\",\"type\":\"STRING\",\"description\":\"Desc 20\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc21\",\"type\":\"STRING\",\"description\":\"Desc 21\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc22\",\"type\":\"STRING\",\"description\":\"Desc 22\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc23\",\"type\":\"STRING\",\"description\":\"Desc 23\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc24\",\"type\":\"STRING\",\"description\":\"Desc 24\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc25\",\"type\":\"STRING\",\"description\":\"Desc 25\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc26\",\"type\":\"STRING\",\"description\":\"Desc 26\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc27\",\"type\":\"STRING\",\"description\":\"Desc 27\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc28\",\"type\":\"STRING\",\"description\":\"Desc 28\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc29\",\"type\":\"STRING\",\"description\":\"Desc 29\"},{\"mode\": \"NULLABLE\",\"name\":\"Desc30\",\"type\":\"STRING\",\"description\":\"Desc 30\"},{\"mode\": \"NULLABLE\",\"name\":\"Value1\",\"type\":\"FLOAT\",\"description\":  \"Value  1\"},{\"mode\": \"NULLABLE\",\"name\":\"Value2\",\"type\":\"FLOAT\",\"description\":  \"Value  2\"},{\"mode\": \"NULLABLE\",\"name\":\"Value3\",\"type\":\"FLOAT\",\"description\":  \"Value  3\"},{\"mode\": \"NULLABLE\",\"name\":\"Value4\",\"type\":\"FLOAT\",\"description\":  \"Value  4\"},{\"mode\": \"NULLABLE\",\"name\":\"Value5\",\"type\":\"FLOAT\",\"description\":  \"Value  5\"},{\"mode\": \"NULLABLE\",\"name\":\"Value6\",\"type\":\"FLOAT\",\"description\":  \"Value  6\"},{\"mode\": \"NULLABLE\",\"name\":\"Value7\",\"type\":\"FLOAT\",\"description\":  \"Value  7\"},{\"mode\": \"NULLABLE\",\"name\":\"Value8\",\"type\":\"FLOAT\",\"description\":  \"Value  8\"},{\"mode\": \"NULLABLE\",\"name\":\"Value9\",\"type\":\"FLOAT\",\"description\":  \"Value  9\"},{\"mode\": \"NULLABLE\",\"name\":\"Value10\",\"type\":\"FLOAT\",\"description\": \"Value 10\"},{\"mode\": \"NULLABLE\",\"name\":\"Value11\",\"type\":\"FLOAT\",\"description\": \"Value 11\"},{\"mode\": \"NULLABLE\",\"name\":\"Value12\",\"type\":\"FLOAT\",\"description\": \"Value 12\"},{\"mode\": \"NULLABLE\",\"name\":\"Value13\",\"type\":\"FLOAT\",\"description\": \"Value 13\"},{\"mode\": \"NULLABLE\",\"name\":\"Value14\",\"type\":\"FLOAT\",\"description\": \"Value 14\"},{\"mode\": \"NULLABLE\",\"name\":\"Value15\",\"type\":\"FLOAT\",\"description\": \"Value 15\"},{\"mode\": \"NULLABLE\",\"name\":\"Value16\",\"type\":\"FLOAT\",\"description\": \"Value 16\"},{\"mode\": \"NULLABLE\",\"name\":\"Value17\",\"type\":\"FLOAT\",\"description\": \"Value 17\"},{\"mode\": \"NULLABLE\",\"name\":\"Value18\",\"type\":\"FLOAT\",\"description\": \"Value 18\"},{\"mode\": \"NULLABLE\",\"name\":\"Value19\",\"type\":\"FLOAT\",\"description\": \"Value 19\"},{\"mode\": \"NULLABLE\",\"name\":\"Value20\",\"type\":\"FLOAT\",\"description\": \"Value 20\"},{\"mode\": \"NULLABLE\",\"name\":\"Value21\",\"type\":\"FLOAT\",\"description\": \"Value 21\"},{\"mode\": \"NULLABLE\",\"name\":\"Value22\",\"type\":\"FLOAT\",\"description\": \"Value 22\"},{\"mode\": \"NULLABLE\",\"name\":\"Value23\",\"type\":\"FLOAT\",\"description\": \"Value 23\"},{\"mode\": \"NULLABLE\",\"name\":\"Value24\",\"type\":\"FLOAT\",\"description\": \"Value 24\"},{\"mode\": \"NULLABLE\",\"name\":\"Value25\",\"type\":\"FLOAT\",\"description\": \"Value 25\"},{\"mode\": \"NULLABLE\",\"name\":\"Value26\",\"type\":\"FLOAT\",\"description\": \"Value 26\"},{\"mode\": \"NULLABLE\",\"name\":\"Value27\",\"type\":\"FLOAT\",\"description\": \"Value 27\"},{\"mode\": \"NULLABLE\",\"name\":\"Value28\",\"type\":\"FLOAT\",\"description\": \"Value 28\"},{\"mode\": \"NULLABLE\",\"name\":\"Value29\",\"type\":\"FLOAT\",\"description\": \"Value 29\"},{\"mode\": \"NULLABLE\",\"name\":\"Value30\",\"type\":\"FLOAT\",\"description\": \"Value 30\"},{\"mode\": \"NULLABLE\",\"name\":\"Flag1\",\"type\":\"INTEGER\",\"description\": \"Flag  1\"},{\"mode\": \"NULLABLE\",\"name\":\"Flag2\",\"type\":\"INTEGER\",\"description\": \"Flag  2\"},{\"mode\": \"NULLABLE\",\"name\":\"Flag3\",\"type\":\"INTEGER\",\"description\": \"Flag  3\"},{\"mode\": \"NULLABLE\",\"name\":\"Flag4\",\"type\":\"INTEGER\",\"description\": \"Flag  4\"},{\"mode\": \"NULLABLE\",\"name\":\"Flag5\",\"type\":\"INTEGER\",\"description\": \"Flag  5\"},{\"mode\": \"NULLABLE\",\"name\":\"Flag6\",\"type\":\"INTEGER\",\"description\": \"Flag  6\"},{\"mode\": \"NULLABLE\",\"name\":\"Flag7\",\"type\":\"INTEGER\",\"description\": \"Flag  7\"},{\"mode\": \"NULLABLE\",\"name\":\"Flag8\",\"type\":\"INTEGER\",\"description\": \"Flag  8\"},{\"mode\": \"NULLABLE\",\"name\":\"Flag9\",\"type\":\"INTEGER\",\"description\": \"Flag  9\"},{\"mode\": \"NULLABLE\",\"name\":\"Flag10\",\"type\":\"INTEGER\",\"description\": \"Flag 10\"},{\"mode\": \"NULLABLE\",\"name\":\"LocationID\",\"type\":\"INTEGER\",\"description\": \"Location ID\"},{\"mode\": \"NULLABLE\",\"name\":\"FillPattern\",\"type\":\"INTEGER\",\"description\": \"Fill pattern\"},{\"mode\": \"NULLABLE\",\"name\":\"XFilename\",\"type\":\"STRING\",\"description\": \"3D Model Filename\"},{\"mode\": \"NULLABLE\",\"name\":\"Segment\",\"type\":\"INTEGER\",\"description\": \"Segment\"},{\"mode\": \"NULLABLE\",\"name\":\"FirstSegment\",\"type\":\"INTEGER\",\"description\": \"First Segment\"},{\"mode\": \"NULLABLE\",\"name\":\"LastSegment\",\"type\":\"INTEGER\",\"description\": \"Last Segment\"},{\"mode\": \"NULLABLE\",\"name\":\"AvailableLinear\",\"type\":\"FLOAT\",\"description\": \"Available linear\"},{\"mode\": \"NULLABLE\",\"name\":\"AvailableSquare\",\"type\":\"FLOAT\",\"description\": \"Available square\"},{\"mode\": \"NULLABLE\",\"name\":\"AvailableCubic\",\"type\":\"FLOAT\",\"description\": \"Available cubic\"},{\"mode\": \"NULLABLE\",\"name\":\"WeightCapacity\",\"type\":\"FLOAT\",\"description\": \"Weight Capacity\"},{\"mode\": \"NULLABLE\",\"name\":\"Notch\",\"type\":\"INTEGER\",\"description\": \"Notch\"},{\"mode\": \"NULLABLE\",\"name\":\"DividerAtStart\",\"type\":\"INTEGER\",\"description\":\"Divider at start\"},{\"mode\": \"NULLABLE\",\"name\":\"DividerAtEnd\",\"type\":\"INTEGER\",\"description\": \"Divider at end\"},{\"mode\": \"NULLABLE\",\"name\":\"DividersBetweenFacings\",\"type\":\"INTEGER\",\"description\": \"Dividers between facings\"},{\"mode\": \"NULLABLE\",\"name\":\"Transparency\",\"type\":\"FLOAT\",\"description\":\"Transparency\"},{\"mode\": \"NULLABLE\",\"name\":\"SpaceAbove\",\"type\":\"FLOAT\",\"description\":\"Space above\"},{\"mode\": \"NULLABLE\",\"name\":\"HideIfPrinting\",\"type\":\"INTEGER\",\"description\":\"Hide if printing\"},{\"mode\": \"NULLABLE\",\"name\":\"ProductAssociation\",\"type\":\"STRING\",\"description\": \"Part ID\"},{\"mode\": \"NULLABLE\",\"name\":\"PartID\",\"type\":\"STRING\",\"description\": \"Product association\"},{\"mode\": \"NULLABLE\",\"name\":\"HideViewDimensions\",\"type\":\"INTEGER\",\"description\": \"Hide View Dimensions\"},{\"mode\": \"NULLABLE\",\"name\":\"GLN\",\"type\":\"STRING\",\"description\": \"GLN\"},{\"mode\": \"NULLABLE\",\"name\":\"CustomData\",\"type\":\"STRING\",\"description\":   \"Custom Data\"},{\"mode\": \"NULLABLE\",\"name\":\"CombinedLinear\",\"type\":\"FLOAT\",\"description\":\"Combined Linear\"},{\"mode\": \"NULLABLE\",\"name\":\"CombinedSquare\",\"type\":\"FLOAT\",\"description\":\"Combined Square\"},{\"mode\": \"NULLABLE\",\"name\":\"CombinedCubic\",\"type\":\"FLOAT\",\"description\": \"Combined Cubic\"},{\"mode\": \"NULLABLE\",\"name\":\"CombinedAvailableLinear\",\"type\":\"FLOAT\",\"description\": \"Combined Available Linear\"},{\"mode\": \"NULLABLE\",\"name\":\"CombinedAvailableSquare\",\"type\":\"FLOAT\",\"description\": \"Combined Available Square\"},{\"mode\": \"NULLABLE\",\"name\":\"CombinedAvailableCubic\",\"type\":\"FLOAT\",\"description\": \"Combined Available Cubic\"},{\"mode\": \"NULLABLE\",\"name\":\"CombinedNumberOfPositions\",\"type\":\"INTEGER\",\"description\":\"Combined number of positions\"},{\"mode\": \"NULLABLE\",\"name\":\"CombinedNumberOfDividers\",\"type\":\"INTEGER\",\"description\": \"Combined number of dividers\"},{\"mode\": \"NULLABLE\",\"name\":\"CanAttach\",\"type\":\"INTEGER\",\"description\": \"Can Attach\"},{\"mode\": \"NULLABLE\",\"name\":\"AttachedToFixture\",\"type\":\"STRING\",\"description\": \"Attached To Fixture\"},{\"mode\": \"NULLABLE\",\"name\":\"IsAttached\",\"type\":\"INTEGER\",\"description\": \"Is Attached\"},{\"mode\": \"NULLABLE\",\"name\":\"RankX\",\"type\":\"INTEGER\",\"description\": \"Rank \"},{\"mode\": \"NULLABLE\",\"name\":\"RankY\",\"type\":\"INTEGER\",\"description\": \"Rank Y\"},{\"mode\": \"NULLABLE\",\"name\":\"RankZ\",\"type\":\"INTEGER\",\"description\": \"Rank Z\"},{\"mode\": \"NULLABLE\",\"name\":\"NumberOfAttachments\",\"type\":\"INTEGER\",\"description\": \" Number Of Attachments\"},{\"mode\": \"NULLABLE\",\"name\":\"CombinedNumberOfAttachments\",\"type\":\"INTEGER\",\"description\": \"Combined Number Of Attachments\"},{\"mode\": \"NULLABLE\",\"name\":\"DBParentFixtureKey\",\"type\":\"INTEGER\",\"description\": \" DB Parent FixtureKey\"},{\"mode\": \"NULLABLE\",\"name\":\"FILE_NAME_DATE\",\"type\":\"STRING\",\"description\": \"file name date\"}]";
		
		 List<String> fileNameList = StorageService.readFileFromStorageFolder(BUCKETNAME, "IX_SPC_FIXTURE_", "fixtureProcess");
		 log.info("Total files" + fileNameList.size());
		 if(fileNameList.size() == 0){		 
			
				log.info("No Files found");
				MailUser.sendMail(PROJECT_NAME + " : DUP_IX_SPC_FIXTURE Notification","No Files found after unzipping"); 
		 }
		 
		 for(int i =0 ; i<fileNameList.size(); i++ ){
				
				
				String dataformat = fileNameList.get(i).replace("IX_SPC_FIXTURE_", "");
			
				dataformat = dataformat.substring(0, 8);
				String tableSpec = PROJECT_NAME + ":JDA_Tables.IX_SPC_FIXTURE";
				String tableErrorSpec  = PROJECT_NAME+":JDA_Error.IX_SPC_FIXTURE_ErrorLog";
				
				
				String successtable = tableSpec + "_" + dataformat;
				String errorTable = tableErrorSpec + "_" + dataformat;

				String filepath = "gs://"+ BUCKETNAME   + "/fixtureProcess/" + fileNameList.get(i);
				StorageService.deletingFileFromStorage(BUCKETNAME, "IX_SPC_FIXTURE_"+dataformat);
				//StorageService.copyingFilefromPath(BUCKETNAME,BUCKETNAME_ARCHIVE,  fileNameList.get(i), "");
				DataFlowProcessForJDA.run(sampleSchema,successtable,errorTable,TEMPLOCATION,filepath,"","DBTime",DelimiterType.PIPE, "IX_SPC_FIXTURE_"+dataformat);		
				DeleteErrorTable.deleteErrorTable("IX_SPC_FIXTURE_ErrorLog_" + dataformat, "JDA_Error",errorTable, "IX_SPC_FIXTURE");	
				DeleteErrorTable.successTableNotification("IX_SPC_FIXTURE_"+ dataformat, "JDA_Tables", successtable, "IX_SPC_FIXTURE");					
				StorageService.deletingFileFromStorageFromProcessing(BUCKETNAME, fileNameList.get(i), "fixtureProcess");
				log.info("Dataflow completed for Product");
			}
		}
		catch(Exception e){
			log.info("Exception Occured : " + e.getLocalizedMessage());
			MailUser.sendMail(PROJECT_NAME + " : DUP_IX_SPC_FIXTURE Notification","Job failed because of below reason : " + e.getMessage()); 
		}
	}
}
