package com.one97.mma.google_sheet_exporter;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.security.GeneralSecurityException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.List;

import javax.imageio.ImageIO;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.ValueRange;

public class App {
	
	private static final String APPLICATION_NAME = "Google Sheets Exporter";
    private static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();
    private static final String TOKENS_DIRECTORY_PATH = "tokens";
    private static final String spreadsheetId = "1vjfmGXyYrIIydFJy_KWb5X7qfcXTFcQoyBp8ILFaATw";
    
    private static final List<String> SCOPES =  Arrays.asList(SheetsScopes.SPREADSHEETS,SheetsScopes.DRIVE);
    private static final String CREDENTIALS_FILE_PATH = "/credentials.json";
    
    
    public static void main(String... args) throws IOException, GeneralSecurityException {
        // Build a new authorized API client service.
        final NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
        final String range = "A2:D3";
        
        Sheets service = getSheetService(HTTP_TRANSPORT);
        
        ValueRange response = service.spreadsheets().values()
                .get(spreadsheetId, range)
                .execute();
        
        List<List<Object>> values = response.getValues();
        
        if (values == null || values.isEmpty()) {
            System.out.println("No data found.");
        } else {
            for (List row : values) {
                String firstName = (String) (row.get(0));
                String lastName = (String) (row.get(1));
                String contactNo = (String) (row.get(2));
                
                String imgId = ((String) row.get(3)).split("/")[5];
                String username = firstName + "-" +lastName;
                
                ByteArrayOutputStream byteArrayOutputStream = downloadImage(imgId, HTTP_TRANSPORT);
                String imgPath = byteArrayToImg(byteArrayOutputStream, username);
                
                //exporting data to MYSQL table
                System.out.println(firstName);
                System.out.println(lastName);
                System.out.println(contactNo);
                System.out.println(imgPath);
                
                exportToDb(firstName, lastName, contactNo, imgPath);
            }
        }
    }
    
    private static Credential authorize(final NetHttpTransport HTTP_TRANSPORT) throws IOException {
        // Load client secrets.
        InputStream in = App.class.getResourceAsStream(CREDENTIALS_FILE_PATH);
        if (in == null) {
            throw new FileNotFoundException("Resource not found: " + CREDENTIALS_FILE_PATH);
        }
        GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(in));

        GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
                HTTP_TRANSPORT, JSON_FACTORY, clientSecrets, SCOPES)
                .setDataStoreFactory(new FileDataStoreFactory(new java.io.File(TOKENS_DIRECTORY_PATH)))
                .setAccessType("offline")
                .build();
        LocalServerReceiver receiver = new LocalServerReceiver.Builder().setPort(8888).build();
        return new AuthorizationCodeInstalledApp(flow, receiver).authorize("user");
    }

    private static Sheets getSheetService(NetHttpTransport HTTP_TRANSPORT) throws IOException , GeneralSecurityException{
    	Sheets service = new Sheets.Builder(HTTP_TRANSPORT, JSON_FACTORY, authorize(HTTP_TRANSPORT))
                .setApplicationName(APPLICATION_NAME)
                .build();
    	return service;
    }
    
    private static Drive getDriveService(NetHttpTransport HTTP_TRANSPORT) throws IOException , GeneralSecurityException{
    	Drive service = new Drive.Builder(HTTP_TRANSPORT, JSON_FACTORY, authorize(HTTP_TRANSPORT))
                .setApplicationName(APPLICATION_NAME)
                .build();
    	return service;
    }
    
    private static ByteArrayOutputStream downloadImage(String realFileId, NetHttpTransport HTTP_TRANSPORT) throws IOException, GeneralSecurityException{
        // Build a new authorized API client service.
        Drive service = getDriveService(HTTP_TRANSPORT);

        try {
            OutputStream outputStream = new ByteArrayOutputStream();

            service.files().get(realFileId)
                    .executeMediaAndDownloadTo(outputStream);

            return (ByteArrayOutputStream) outputStream;
        }catch (GoogleJsonResponseException e) {
            System.err.println("Unable to move file: " + e.getDetails());
            throw e;
        }
    }
    
    private static String byteArrayToImg(ByteArrayOutputStream byteArrayOutputStream, String username) throws IOException {
    	byte [] byteArray = byteArrayOutputStream.toByteArray();
        
        ByteArrayInputStream inStreambj = new ByteArrayInputStream(byteArray);
       
        // read image from byte array
        BufferedImage newImage = ImageIO.read(inStreambj);
         
        // write output image
        String path = "D:/Images/"+ username +".jpg";
        ImageIO.write(newImage, "jpg", new File(path));
        System.out.println("Image downloaded");
        
        return path;
    }
    
    
    private static void exportToDb(String firstName, String lastName, String contactNo, String imagePath) {
    	String JdbcURL = "jdbc:mysql://localhost:3306/mma_info";
        String Username = "root";
        String password = "12345";
        Connection con = null;
        PreparedStatement pstmt = null;
        
        String query = "INSERT INTO user_info(first_name, last_name, contact_no, image_path)" + "VALUES (?, ?, ?,?)";
        
        try {
           Class.forName("com.mysql.cj.jdbc.Driver");
           con = DriverManager.getConnection(JdbcURL, Username, password);
           
           pstmt = con.prepareStatement(query);
           pstmt.setString(1, firstName);
           pstmt.setString(2, lastName);
           pstmt.setString(3, contactNo);
           pstmt.setString(4, imagePath);
           
           int status = pstmt.executeUpdate();
           if(status > 0) {
              System.out.println("Record is inserted successfully !!!");
           }
        } catch(Exception e){
           e.printStackTrace();
        }
    }
    
    
}