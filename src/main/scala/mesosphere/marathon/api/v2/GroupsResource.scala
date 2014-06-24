package mesosphere.marathon.api.v2

import javax.ws.rs._
import javax.ws.rs.core.{ Request, Response, MediaType }
import javax.inject.Inject
import javax.validation.{ ConstraintViolation, ConstraintViolationException, Validation }
import mesosphere.marathon.state.{ PathId, Group, Timestamp, GroupManager }
import scala.concurrent.Await.result
import scala.concurrent.duration._
import mesosphere.marathon.api.Responses
import scala.collection.JavaConverters._
import org.hibernate.validator.internal.engine.ConstraintViolationImpl
import java.lang.annotation.ElementType
import org.hibernate.validator.internal.engine.path.PathImpl
import scala.reflect.ClassTag
import scala.collection.mutable
import PathId._
import mesosphere.util.ThreadPoolContext.context

@Path("v2/groups")
@Produces(Array(MediaType.APPLICATION_JSON))
class GroupsResource @Inject() (groupManager: GroupManager) {

  val defaultWait = 3.seconds
  val ListVersionsRE = """^(.+)/versions$""".r
  val GetVersionRE = """^(.+)/versions/(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z)$""".r

  /**
    * Get root group.
    */
  @GET
  def root(): Group = result(groupManager.root, defaultWait)

  /**
    * Get a specific group, optionally with specifc version
    * @param id the identifier of the group encded as path
    * @return the group or the group versions.
    */
  @GET
  @Path("""{id:.+}""")
  def group(@PathParam("id") id: String): Response = {
    def groupResponse(g: Option[Group]) = g match {
      case Some(group) => Response.ok(group).build()
      case None        => Responses.unknownGroup(id.toRootPath)
    }
    id match {
      case ListVersionsRE(id)        => Response.ok(result(groupManager.versions(id.toRootPath), defaultWait)).build()
      case GetVersionRE(id, version) => groupResponse(result(groupManager.group(id.toRootPath, Timestamp(version)), defaultWait))
      case _                         => groupResponse(result(groupManager.group(id.toRootPath), defaultWait))
    }
  }

  /**
    * Create a new group.
    * @param update the group is encoded in the update.
    */
  @POST
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def create(update: GroupUpdate): Response = {
    require(update.id.isDefined)
    updateOrCreate(PathId.empty, update, force = false)
  }

  /**
    * Create or update a group.
    * If the path to the group does not exist, it gets created.
    * @param id is the identifier of the the group to update.
    * @param update is the update to apply on the group specified by the given path.
    * @param force if the change has to be forced. A running upgrade process will be halted and the new one is started.
    */
  @POST
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Path("""{id:.+}""")
  def createUpdate(@PathParam("id") id: String,
                   update: GroupUpdate,
                   @DefaultValue("false")@QueryParam("force") force: Boolean): Response = {
    updateOrCreate(id.toRootPath, update, force)
  }

  /**
    * Create or update a group.
    * If the path to the group does not exist, it gets created.
    * @param id is the identifier of the the group to update.
    * @param update is the update to apply on the group specified by the given path.
    * @param force if the change has to be forced. A running upgrade process will be halted and the new one is started.
    */
  @PUT
  @Consumes(Array(MediaType.APPLICATION_JSON)) //@Path("""{path:(?!.*/version/\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$).+}""")
  @Path("""{id:.+}""")
  def update(@PathParam("id") id: String,
             update: GroupUpdate,
             @DefaultValue("false")@QueryParam("force") force: Boolean): Response = {
    updateOrCreate(id.toRootPath, update, force)
  }

  /**
    * Rollback to a specific version of a given group.
    * @param id the identifier of the group to roll back.
    * @param version the version of the group to roll to.
    * @param force if there is an upgrade in progress, it can be overriden with the force flag.
    */
  @PUT
  @Path("""{id:.+}/version/{version}""")
  def rollbackTo(@PathParam("id") id: String,
                 @PathParam("version") version: String,
                 @DefaultValue("false")@QueryParam("force") force: Boolean): Response = {
    val groupId = id.toRootPath
    val res = groupManager.group(groupId, Timestamp(version)).map {
      case Some(group) =>
        groupManager.update(groupId, group.version, _ => group, force)
        Response.noContent().build()
      case None =>
        Responses.unknownGroup(groupId)
    }
    result(res, defaultWait)
  }

  /**
    * Delete a specific subtree or a complete tree.
    * @param id the identifier of the group to delete encoded as path
    * @param force if the change has to be forced. A running upgrade process will be halted and the new one is started.
    * @return A version response, which defines the resulting change.
    */
  @DELETE
  @Path("""{id:.+}""")
  def delete(@PathParam("id") id: String,
             @DefaultValue("false")@QueryParam("force") force: Boolean): Response = {
    val groupId = id.toRootPath
    val version = Timestamp.now()
    groupManager.update(groupId.rootPath, version, _.remove(groupId, version), force)
    Response.ok(Map("version" -> version)).build()
  }

  private def updateOrCreate(id: PathId, update: GroupUpdate, force: Boolean): Response = {
    checkIsValid(update)
    val version = Timestamp.now()
    val effectivePath = update.id.map(_.canonicalPath(id)).getOrElse(id)
    groupManager.update(effectivePath, version, group => update.apply(group, version), force)
    Response.ok(Map("version" -> version)).build()
  }

  //Note: this is really ugly. It is necessary, since bean validation will not walk into a scala Seq[_] and
  //can not check scala Double values. So we have to do this by hand.
  val validator = Validation.buildDefaultValidatorFactory().getValidator
  private def checkIsValid(root: GroupUpdate) {
    def withPath[T](bean: T, e: ConstraintViolation[_], path: String)(implicit ct: ClassTag[T]): ConstraintViolation[T] = {
      ConstraintViolationImpl.forParameterValidation[T](
        e.getMessageTemplate, e.getMessage, ct.runtimeClass.asInstanceOf[Class[T]], bean, e.getLeafBean, e.getInvalidValue,
        PathImpl.createPathFromString(path + e.getPropertyPath),
        e.getConstraintDescriptor, ElementType.FIELD, e.getExecutableParameters)
    }
    def groupValidation(path: String, group: GroupUpdate): mutable.Set[ConstraintViolation[GroupUpdate]] = {
      val groupErrors = validator.validate(group).asScala.map(withPath(root, _, path))
      val appErrors = group.apps
        .getOrElse(Seq.empty)
        .flatMap(app => validator.validate(app).asScala)
        .zipWithIndex
        .map(a => withPath(root, a._1, path + s"apps[${a._2}]."))
      val nestedGroupErrors = group.groups
        .getOrElse(Seq.empty)
        .zipWithIndex
        .flatMap(g => groupValidation(path + s"groups[${g._2}].", g._1))
      val healthErrors = group.scalingStrategy.map { scalingStrategy =>
        val capacityErrors = {
          if (scalingStrategy.minimumHealthCapacity < 0) Some("is less than 0")
          else if (scalingStrategy.minimumHealthCapacity > 1) Some("is greater than 1")
          else None
        } map { msg =>
          ConstraintViolationImpl.forParameterValidation[GroupUpdate](
            msg, msg, classOf[GroupUpdate], group, group.scalingStrategy, group.scalingStrategy,
            PathImpl.createPathFromString(path + "scalingStrategy.minimumHealthCapacity"),
            null, ElementType.FIELD, Array())
        }
        val scalingErrors = scalingStrategy.maximumRunningFactor.collect {
          case x if x < 1                                      => "is less than 1"
          case x if x <= scalingStrategy.minimumHealthCapacity => "is less than or equal to minimumHealthCapacity"
        } map { msg =>
          ConstraintViolationImpl.forParameterValidation[GroupUpdate](
            msg, msg, classOf[GroupUpdate], group, group.scalingStrategy, group.scalingStrategy,
            PathImpl.createPathFromString(path + "scalingStrategy.maximumRunningFactor"),
            null, ElementType.FIELD, Array())
        }
        capacityErrors ++ scalingErrors
      }.getOrElse(Nil)
      groupErrors ++ nestedGroupErrors ++ appErrors ++ healthErrors
    }

    val errors = groupValidation("", root)
    if (errors.nonEmpty) throw new ConstraintViolationException("Group is not valid", errors.asJava)
  }
}
