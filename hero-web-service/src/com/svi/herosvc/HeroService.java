package com.svi.herosvc;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
//import org.json.JSONException;
import org.json.JSONObject;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.svi.gfs.main.Gfs;
import com.svi.gfs.object.FileObject;
//import com.svi.gfs.upload.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
//import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/service")
public class HeroService {

	private String urlString = "http://localhost:8983/solr/hero_details";
	private SolrClient Solr = new HttpSolrClient.Builder(urlString).build();

	private Cluster cluster = Cluster.builder().addContactPoint("192.168.143.19").build();
	private Session session = cluster.connect("sample");

	String gfsEnvironmentLocation = "C:\\Users\\jdeopante\\Documents\\BPO\\fromGfs";
	Gfs gfs = Gfs.newLocalStorageBuilder(gfsEnvironmentLocation).build();
	List<FileObject> files = new ArrayList<>();

	@Path("/create")
	@POST
	public Response create(String jsonObject) {

		String heroId = "";
		String heroName = "";
		String heroRole = "";
		String heroSpecialty = "";
		String heroPrice = "";

		JSONObject heroObj = new JSONObject(jsonObject);

		if (heroObj.has("hero_id")) {
			heroId = (String) heroObj.get("hero_id");
		}
		;

		if (heroObj.has("hero_name")) {
			heroName = (String) heroObj.get("hero_name");
		}
		;

		if (heroObj.has("hero_role")) {
			heroRole = (String) heroObj.get("hero_role");
		}
		;

		if (heroObj.has("hero_specialty")) {
			heroSpecialty = (String) heroObj.get("hero_specialty");
		}
		;

		if (heroObj.has("hero_price")) {
			heroPrice = (String) heroObj.get("hero_price");
		}
		;

		SolrInputDocument doc = new SolrInputDocument();

		doc.addField("hero_id", heroId);
		doc.addField("hero_name", heroName);
		doc.addField("hero_role", heroRole);
		doc.addField("hero_specialty", heroSpecialty);

		try {

			String add = "INSERT INTO hero_details (hero_id, hero_name, hero_role, hero_specialty, hero_price)"
					+ " VALUES (" + heroId + " , '" + heroName + "' , '" + heroRole + "' , '" + heroSpecialty + "' , '"
					+ heroPrice + "');";

			session.execute(add);

			Solr.add(doc);
			Solr.commit();

		} catch (SolrServerException e) {

			e.printStackTrace();
		} catch (IOException e) {

			e.printStackTrace();
		}

		return Response
				.ok("Hero added : " + heroId + " " + heroName + " " + heroRole + " " + heroSpecialty + " " + heroPrice)
				.build();

	}

	@Path("/update")
	@PUT
	public Response update(String jsonObject) {

		JSONObject heroObj = new JSONObject(jsonObject);

		String heroId = "";
		String heroName = "";
		String heroRole = "";
		String heroSpecialty = "";
		String heroPrice = "";

		if (heroObj.has("hero_id")) {
			heroId = (String) heroObj.get("hero_id");
		}
		;

		if (heroObj.has("hero_name")) {
			heroName = (String) heroObj.get("hero_name");
		}
		;

		if (heroObj.has("hero_role")) {
			heroRole = (String) heroObj.get("hero_role");
		}
		;

		if (heroObj.has("hero_specialty")) {
			heroSpecialty = (String) heroObj.get("hero_specialty");
		}
		;

		if (heroObj.has("hero_price")) {
			heroPrice = (String) heroObj.get("hero_price");
		}
		;

		SolrQuery query = new SolrQuery("hero_id" + ":" + heroId);

		try {
			QueryResponse response = Solr.query(query);
			SolrDocument oldDoc = response.getResults().get(0);
			SolrInputDocument newDoc = new SolrInputDocument();

			String oldId = (String) oldDoc.getFieldValue("hero_id");
			String oldName = (String) oldDoc.getFieldValue("hero_name");
			String oldRole = (String) oldDoc.getFieldValue("hero_role");
			String oldSpecialty = (String) oldDoc.getFieldValue("hero_specialty");

			if (heroId == "") {
				heroId = oldId;
			}
			;

			if (heroName == "") {
				heroName = oldName;
			}
			;

			if (heroRole == "") {
				heroRole = oldRole;
			}
			;

			if (heroSpecialty == "") {
				heroSpecialty = oldSpecialty;
			}
			;

			newDoc.setField("hero_id", oldId);
			newDoc.setField("hero_name", oldName);
			newDoc.setField("hero_role", oldRole);
			newDoc.setField("hero_specialty", oldSpecialty);

			newDoc.setField("hero_id", heroId);
			newDoc.setField("hero_name", heroName);
			newDoc.setField("hero_role", heroRole);
			newDoc.setField("hero_specialty", heroSpecialty);

			Solr.add(newDoc);
			Solr.commit();

			String updateField = " UPDATE hero_details SET hero_name =" + "'" + heroName + "'" + ", hero_role =" + "'"
					+ heroRole + "'" + ", hero_specialty =" + "'" + heroSpecialty + "'" + ", hero_price =" + "'"
					+ heroPrice + "' WHERE hero_id=" + oldId;

			session.execute(updateField);

		} catch (SolrServerException e1) {

			e1.printStackTrace();
		} catch (IOException e1) {

			e1.printStackTrace();
		}

		System.out.println("Documents added");

		return Response.ok(
				"updated: " + " " + heroId + " " + heroName + " " + heroRole + " " + heroSpecialty + " " + heroPrice)
				.build();

	}

	@Path("/read/{param1}/{param2}")
	@GET
	public Response read(@PathParam("param1") String field, @PathParam("param2") String fieldValue) {

		String readHero = "";
		QueryResponse response = null;
		
		Object oldId = null;
		
		JSONObject responseJsonObj = new JSONObject();

		int responseCount = 0;

		SolrQuery query = new SolrQuery(field + ":" + fieldValue);

		try {
			response = Solr.query(query);
			responseCount = response.getResults().size();

			SolrDocumentList oldDoc = response.getResults();

			for (int i = 0; i < responseCount; i++) {

				oldId = oldDoc.get(i).getFieldValue("hero_id");

				readHero = " SELECT * FROM hero_details WHERE hero_id=" + oldId;

				ResultSet result = null;
				JSONObject heroJsonObj = new JSONObject();
				result = session.execute(readHero);		
				
				for (Row row : result) {

					heroJsonObj.append("hero_id", row.getVarint(0).toString());
					heroJsonObj.append("hero_name", row.getString(1).toString());
					heroJsonObj.append("hero_price", row.getString(2).toString());
					heroJsonObj.append("hero_role", row.getString(3).toString());
					heroJsonObj.append("hero_specialty", row.getString(4).toString());

					responseJsonObj.append(oldId + "_details", heroJsonObj);

				}

				Solr.commit();
			}
			
			
			

		} catch (SolrServerException e) {

			e.printStackTrace();
		} catch (IOException e) {

			e.printStackTrace();
		}

		return Response.ok(responseJsonObj.toString()).build();
	}

	@Path("/delete/{param1}/{param2}")
	@DELETE
	public Response delete(@PathParam("param1") String field, @PathParam("param2") String fieldValue) {
		QueryResponse response = null;
		int responseCount = 0;
		Object heroId = null;
		String deletedHeroes = "";
		ResultSet result = null;
		String results = "";

		SolrQuery query = new SolrQuery(field + ":" + fieldValue);

		try {
			Solr.deleteByQuery(field + ":" + fieldValue);

			response = Solr.query(query);
			responseCount = response.getResults().size();
			SolrDocumentList oldDoc = response.getResults();

			for (int i = 0; i < responseCount; i++) {

				heroId = oldDoc.get(i).getFieldValue("hero_id");
				
				deletedHeroes += oldDoc.get(i).getFieldValue("hero_name").toString() + ",";

				String deleteHero = "DELETE FROM hero_details where hero_id= " + heroId;

				session.execute(deleteHero);

				Solr.commit();
			}

			// Solr.deleteByQuery();
		} catch (SolrServerException e) {

			e.printStackTrace();
		} catch (IOException e) {

			e.printStackTrace();
		}

		return Response.ok("deleted: " + deletedHeroes).build();
	}

	@Path("/upload/file/{param1}")
	@POST
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response uploadFile(@PathParam("param1") String key, @FormDataParam("file") InputStream uploadedInputStream,
			@FormDataParam("file") FormDataContentDisposition fileDetail) {

		String fileNameAndType = fileDetail.getFileName().toString();
		String[] partsOfFileNameAndType = fileNameAndType.split("\\.");
		String fileName = partsOfFileNameAndType[0];
		String fileType = partsOfFileNameAndType[1];

		ByteArrayOutputStream ouputStream = new ByteArrayOutputStream();

		int ctr;

		try {
			while ((ctr = uploadedInputStream.read()) != -1) {
				ouputStream.write(ctr);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		byte[] bytes = ouputStream.toByteArray();
		byte[] fileBlob = bytes;

		UUID uuid = UUID.randomUUID();

		FileObject file = new FileObject();

		file.setFileId(uuid.toString());
		file.setFileName(fileName);
		file.setFileType(fileType);
		file.setFileBlob(fileBlob);

		files.add(file);

		gfs.upload(key, files);

		String output = "File successfully uploaded: " + fileNameAndType;

		return Response.ok(output).build();

	}

	@Path("/update/file/{param1}")
	@PUT
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response updateFile(@PathParam("param1") String key, @FormDataParam("file") InputStream uploadedInputStream,
			@FormDataParam("file") FormDataContentDisposition fileDetail) {

		String fileNameAndType = fileDetail.getFileName().toString();
		String[] partsOfFileNameAndType = fileNameAndType.split("\\.");
		String fileName = partsOfFileNameAndType[0];
		String fileType = partsOfFileNameAndType[1];

		ByteArrayOutputStream ouputStream = new ByteArrayOutputStream();

		int ctr;

		try {
			while ((ctr = uploadedInputStream.read()) != -1) {
				ouputStream.write(ctr);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		byte[] bytes = ouputStream.toByteArray();
		byte[] fileBlob = bytes;

		UUID uuid = UUID.randomUUID();

		FileObject file = new FileObject();

		file.setFileId(uuid.toString());
		file.setFileName(fileName);
		file.setFileType(fileType);
		file.setFileBlob(fileBlob);

		files.add(file);

		gfs.update(key, files);

		return Response.ok("File Updated: " + key).build();
	}

	@Path("/retrieve/file/{param1}")
	@GET
	public Response retrieveFileIdName(@PathParam("param1") String fileKey) {

		List<FileObject> filesRetrieved = null;

		boolean keyExists = gfs.exists(fileKey);

		if (keyExists = true) {
			filesRetrieved = gfs.retrieve(fileKey);
		}

		JSONObject responseJsonObj = new JSONObject();
		

		for (FileObject file : filesRetrieved) {

			JSONObject fileJsonObj = new JSONObject();
			
			fileJsonObj.append("file_id", file.getFileId());
			fileJsonObj.append("file_name", file.getFileName());
			fileJsonObj.append("file_type", file.getFileType());
			fileJsonObj.append("file_blob", file.getFileBlob().toString());

			responseJsonObj.append(fileKey + "_details", fileJsonObj);

		}

		return Response.ok(responseJsonObj.toString()).build();
	}

	@Path("/download/file/{param1}/{param2}")
	@GET
	@Produces()
	public Response downloadFile(@PathParam("param1") String fileKey, @PathParam("param2") String fileId) {

		String message = "";
		String idOfFile = "";
		if (gfs.exists(fileKey)) {
			files = gfs.retrieve(fileKey);
			for (FileObject file : files) {
				idOfFile = file.getFileId().toString();
				String nameOfFile = file.getFileName().toString();
				String typeOfFile = file.getFileType().toString();
				byte[] blobOfFile = file.getFileBlob();
				if (fileId.equals(idOfFile)) {
					try {
						FileOutputStream out = new FileOutputStream("C:\\GFS\\" + nameOfFile + "." + typeOfFile);
						out.write(blobOfFile);
						out.close();
						message = "File downloaded successfully";
					} catch (IOException e) {
						message = e.getMessage();
					}

				} else {
					message = "file ID does not exist";
				}
			}
		} else {
			message = "key does not exist";
		}

		return Response.ok(message).build();

	}

}
