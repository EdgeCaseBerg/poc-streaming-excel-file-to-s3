package services

import models.AWSAccess
import org.apache.poi.xssf.streaming.SXSSFWorkbook
import org.apache.poi.xssf.streaming.GZIPSheetDataWriter
import org.apache.poi.xssf.streaming.SheetDataWriter
import org.apache.poi.ss.usermodel.Cell
import org.apache.poi.ss.usermodel.Row
import org.apache.poi.ss.usermodel.Sheet
import org.apache.poi.ss.usermodel.Workbook
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.poi.ss.util.CellReference
import org.apache.commons.io.IOUtils

import java.io.FileOutputStream
import java.io.OutputStream
import java.io.PipedOutputStream
import java.io.PipedInputStream
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream

import java.io.File
import java.io.IOException
import java.util.ArrayList
import java.util.List
import java.util.zip.ZipEntry
import java.util.zip.ZipFile
import java.util.zip.ZipOutputStream

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.internal.StaticCredentialsProvider
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

/** 
 * Proof of concept code, may contain mess or un-disposed of resources 
 */
class ExcelStreamingToS3Service (awsAccess: AWSAccess) {
	val maxRowsInMemory: Int = 200 // Low number just to force things
	
	/** Set part size to 5 MB.
	 * @see https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
	 */
	val minimumPartSize = 5 * 1024 * 1024;

	def readAndUpload(pi: PipedInputStream) = {
		/* It's important to not be on the same thread when reading from the piped input stream */
		Future {
			val clientRegion = "us-west-2";
			val bucketName = "ew-test-bucket";
			val keyName = "test/streamed-excel.xlsx";

			try {

				val s3Client = AmazonS3ClientBuilder.standard()
									.withRegion(clientRegion)
									.withCredentials(new StaticCredentialsProvider(new BasicAWSCredentials(awsAccess.key, awsAccess.secret)))
									.build()

				/* Initiate the upload 
				 * @see https://docs.aws.amazon.com/AmazonS3/latest/dev/llJavaUploadFile.html
				 */
				val initRequest: InitiateMultipartUploadRequest = new InitiateMultipartUploadRequest(bucketName, keyName);
				val initResponse: InitiateMultipartUploadResult = s3Client.initiateMultipartUpload(initRequest);

				/* First create the template/wrapper parts of the file. 
				 * This is basically
				 *
				 * @see http://web.archive.org/web/20110821054135/http://www.realdevelopers.com/blog/code/excel
				 * @see http://web.archive.org/web/20120525160349/http://www.docjar.org/html/api/org/apache/poi/xssf/usermodel/examples/BigGridDemo.java.html
				 * @see https://poi.apache.org/apidocs/org/apache/poi/xssf/streaming/SheetDataWriter.html
				 */
				val templateWorkBook = new XSSFWorkbook()
				val templateSheet = templateWorkBook.createSheet("data")

				/* If we wanted custom styles in the sheet we'd insert them into the template here like in the BigGridDemo.java file around line 55 
				 * but for this proof of concept we won't bother, rather we just need to get the sheet reference
				 */
				val sheetReference = templateSheet.getPackagePart().getPartName().getName()
			   
				/* Now save the data from the workbook into a temporary file for us 
				 * TODO: Use byte stream to keep things in memory maybe?
				 */
				val templateFileOutputStream = new FileOutputStream("template.xlsx")
				templateWorkBook.write(templateFileOutputStream)

				templateFileOutputStream.close()
				templateWorkBook.close()

				/* Now the tricky part, 
				 * we have a PipedInputStream/PipedOutputStream that generates the raw XML for the sheet's data
				 * but we need to upload the other parts of the zipfile (xlsx) first, then append the sheet xml
				 * as the last entry in the zip.
				 * So first we upload those template parts
				 */
				val sheetToSkip = sheetReference.substring(1) // Do this because the BigGridDemo did it.
				val zipFile = new ZipFile(new File("template.xlsx"))
				val templateBytes = new ByteArrayOutputStream(1024 * 1024)
				val zos = new ZipOutputStream(templateBytes);

				try {
					val enumerationOfZipEntries = zipFile.entries()
					while(enumerationOfZipEntries.hasMoreElements()) {
						val zipEntry = enumerationOfZipEntries.nextElement()
						/* If it's not the sheet we're going to substitute, then add it in */
						if (!zipEntry.getName().equals(sheetToSkip)) {
							zos.putNextEntry(new ZipEntry(zipEntry.getName()))
							/* Copy the input stream to the output stream */
							val zipEntryInputStream = zipFile.getInputStream(zipEntry)
							try {
								IOUtils.copy(zipEntryInputStream, zos)
							} finally {
								zipEntryInputStream.close()
							}
							// No need to call closeEntry becuase putNextEntry closes the current active entry
						}
					}

					/* Next create a zip entry for the sheet we're about to get and start the sheet xml */
					val sheetZipEntry = new ZipEntry(sheetToSkip)
					zos.putNextEntry(sheetZipEntry)
					/* Write the starting elements for the worksheet */
					zos.write({
						"<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + 
						"<worksheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\">" +
						"<sheetData>\n"
					}.getBytes())
					zos.flush()

					// Upload the file parts.
					val partETags = new ArrayList[PartETag]()
					var byteBuffer = Array.ofDim[Byte](0)
					var bytesRead = 0 // Get into the loop
					var partNumber = 1
					/* Do while because we must read it once */
					do {
						byteBuffer = Array.ofDim[Byte](minimumPartSize)
						bytesRead = pi.read(
							byteBuffer, // into this array
							0,  // start offset into the destination byteBuffer
							minimumPartSize // read up to 5mb out at a time
						)

						/* Zip the byte buffer data */
						zos.write(byteBuffer)
						zos.flush()

						// Create the request to upload a part.
						if(templateBytes.size() >= minimumPartSize || bytesRead == -1) {
							if (bytesRead == -1) {
								println("Finishing off sheet!")
								/* Last read, so this should be the last part to upload.
								 * Therefore, finish off the xml elements for the sheet
								 */
								zos.write("</sheetData></worksheet>".getBytes())
								zos.closeEntry()
								zos.flush()
								zos.finish()
								zos.close()							
							}

							zos.flush()

							val zippedBytes = templateBytes.toByteArray()
							val uploadSize = zippedBytes.size

							println("Upload size: " + uploadSize)
							// We have another bytes for a part, make it:
							val uploadRequest: UploadPartRequest  = new UploadPartRequest()
								.withBucketName(bucketName)
								.withKey(keyName)
								.withUploadId(initResponse.getUploadId())
								.withPartNumber(partNumber)
								.withInputStream(new ByteArrayInputStream(zippedBytes))
								.withPartSize(uploadSize);

							val uploadResult: UploadPartResult = s3Client.uploadPart(uploadRequest);
							partETags.add(uploadResult.getPartETag());
							println("PART: " + partNumber + " Etag: " + uploadResult.getPartETag())

							// Reset byteBuffer and partNumber for next iteration
							partNumber = partNumber + 1
							templateBytes.reset() //TODO figure out how to deal with this/
						} else {
							// We don'thave enough bytes yet, so keep on accumulating
							println("Fetching more data from buffer..., read: " + bytesRead + " bytes last read.")
						}
					} while(bytesRead != -1) 

					println("Completing!")

					// Complete the multipart upload.
					val compRequest: CompleteMultipartUploadRequest = new CompleteMultipartUploadRequest(
						bucketName, 
						keyName,
						initResponse.getUploadId(), 
						partETags
					);
					s3Client.completeMultipartUpload(compRequest);
				} finally {
					zos.close()
					zipFile.close()
				}
			} catch {
				case e: Throwable => 
					e.printStackTrace()

			} finally {
				pi.close()
			}
		}
	}

	// https://poi.apache.org/components/spreadsheet/how-to.html#sxssf
	def start() {
		/* This input stream will slowly fill up with the excel files contents. */
		val pi = new PipedInputStream(1024 * 1024)
		/* Decorate the output stream so we can hook into it with the pipe,
		 * that way, as the data is created and sent out to the temporary files
		 * we also capture the input and send it out to s3. Since the java doc
		 * isn't really clear if I get a copy and the rest of the data goes out
		 * to the file or not, we may or may not be writing to temporary files or
		 * intercepting it. I haven't looked at the underlying source so I don't know.
		 * @see https://poi.apache.org/apidocs/org/apache/poi/xssf/streaming/SheetDataWriter.html#decorateOutputStream-java.io.FileOutputStream-
		 */
		val workbook = new SXSSFWorkbook(maxRowsInMemory) {
			override protected def createSheetDataWriter(): SheetDataWriter = {
				if(isCompressTempFiles()) {
					return new GZIPSheetDataWriter(getSharedStringSource()) {
						override protected def decorateOutputStream(fos: FileOutputStream): OutputStream = {
							new PipedOutputStream(pi)
						}
					}
				}

				return new SheetDataWriter(getSharedStringSource()) {
					override protected def decorateOutputStream(fos: FileOutputStream): OutputStream = {
						new PipedOutputStream(pi)
					}
				}
			}
		}
		/* 
		 * SXSSF writes sheet data in temporary files (a temp file per-sheet) and the size of these 
		 * temp files can grow to to a very large size, e.g. for a 20 MB csv data the size of the 
		 * temp xml file become few GB large. If the "compress" flag is set to true then the 
		 * temporary XML is gzipped.
		 */
		workbook.setCompressTempFiles(true)
		try {
			val sheet = workbook.createSheet()
			readAndUpload(pi)

			/* Generate a bunch of stuff in the excel sheet until we're done. */
			(1 to 7500).foreach { rowNumber =>
				val row = sheet.createRow(rowNumber)
				(1 to 10).foreach { cellNumber =>
					val cell = row.createCell(cellNumber)
					cell.setCellValue(rowNumber + "x" + cellNumber)
				}
				if (rowNumber % 1000 == 0) {
					println("Row: " + rowNumber)
				}
			}

		} finally {
			println("Done writing excel sheet")
			/* 
			 Note that SXSSF allocates temporary files that you must always clean up explicitly, by calling the dispose method.
			 */
			workbook.dispose()
			workbook.close()
		}
	}
}