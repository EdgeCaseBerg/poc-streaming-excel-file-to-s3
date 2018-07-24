package controllers

import javax.inject.Inject

import models.AWSAccess
import services.ExcelStreamingToS3Service
import play.api.data._
import play.api.i18n._
import play.api.mvc._

/**
 * The classic WidgetController using MessagesAbstractController.
 *
 * Instead of MessagesAbstractController, you can use the I18nSupport trait,
 * which provides implicits that create a Messages instance from a request
 * using implicit conversion.
 *
 * See https://www.playframework.com/documentation/2.6.x/ScalaForms#passing-messagesprovider-to-form-helpers
 * for details.
 */
class PoCController @Inject()(cc: MessagesControllerComponents) extends MessagesAbstractController(cc) {
  import PoCForm._

  private val postUrl = routes.PoCController.startUpload()

  def index = Action {
    Ok(views.html.index())
  }

  def startUploadForm = Action { implicit request =>
    Ok(views.html.startForm(PoCForm.form, postUrl))
  }

  def startUpload = Action { implicit request: MessagesRequest[AnyContent] =>
    val errorFunction = { formWithErrors: Form[Data] =>
      BadRequest(views.html.startForm(formWithErrors, postUrl))
    }

    val successFunction = { data: Data =>
      // This is the good case, where the form was successfully parsed as a Data object.
      val creds = AWSAccess(key = data.accessKey, secret = data.accessSecret)
      val estss = new ExcelStreamingToS3Service(creds)
      estss.start()
      Redirect(routes.PoCController.startUploadForm()).flashing("info" -> "Streaming started!")
    }

    val formValidationResult = form.bindFromRequest
    formValidationResult.fold(errorFunction, successFunction)
  }
}
