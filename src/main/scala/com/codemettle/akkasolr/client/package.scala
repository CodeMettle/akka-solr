package com.codemettle.akkasolr

import com.codemettle.akkasolr.solrtypes.SolrQueryResponse

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by steven on 5/17/2018.
  */
package object client {
  implicit class RichResponse(val res: SolrQueryResponse) extends AnyVal {
    def toFailMessage(implicit opts: Solr.UpdateOptions): Either[Solr.UpdateError, SolrQueryResponse] =
      res.status match {
        case status if status != 0 && opts.failOnNonZeroStatus => Left(Solr.UpdateError(status, res.errorMessageOpt))
        case _ => Right(res)
      }
  }

  implicit class RichResponseFuture(val resF: Future[SolrQueryResponse]) extends AnyVal {
    def failIfNeeded(implicit opts: Solr.UpdateOptions, ec: ExecutionContext): Future[SolrQueryResponse] =
      resF.map(_.toFailMessage) flatMap {
        case Left(err) => Future.failed(err)
        case Right(res) => Future.successful(res)
      }
  }
}
