﻿module Gallery.CsvService.DataProviders

open System
open Suave
open Suave.Filters
open Suave.Operators
open Gallery.CsvService
open Gallery.CsvService.Storage
open FSharp.Data
open WebScrape.DataProviders

let xcookie f ctx = async {
  match ctx.request.headers |> Seq.tryFind (fun (h, _) -> h.ToLower() = "x-cookie") with
  | Some(_, v) -> 
      let cks = v.Split([|"&"|], StringSplitOptions.RemoveEmptyEntries) |> Array.map (fun k -> 
        match k.Split('=') with [|k;v|] -> k, v | _ -> failwith "Wrong cookie!") |> dict
      return! f cks ctx
  | _ -> return None }

let handleRequest root =
  choose [
    path "/providers/data/" >=> request (fun r ->
      Serializer.returnMembers [
        Member("load", Some [Parameter("url", Type.Named("string"), false, ParameterKind.Static("url"))], Result.Nested("/upload"), [], [])
        Member("scrape", Some [Parameter("url", Type.Named("string"), false, ParameterKind.Static("url"))], Result.Nested("/scrape"), [], [])
      ])

    path "/providers/data/upload" >=> xcookie (fun ck ctx -> async {
      use wc = new System.Net.WebClient()
      let url = ck.["url"]
      let! file = wc.AsyncDownloadString(Uri(url))
      let! upload = Storage.Cache.uploadFile url file 
      match upload with 
      | Choice2Of2 msg -> return! RequestErrors.BAD_REQUEST msg ctx
      | Choice1Of2 id ->
          return! ctx |> Serializer.returnMembers [
            Member("explore", None, Result.Provider("pivot", root + "/providers/data/query/" + id), [], [])
          ] })

    pathScan "/providers/data/query/%s" (fun id ctx -> async {
      let! file = Storage.Cache.fetchFile id
      match file with 
      | None ->
          return! RequestErrors.BAD_REQUEST "File has not been uploaded." ctx
      | Some(meta, data) ->
          return! Pivot.handleRequest meta data (List.map fst ctx.request.query) ctx }
    )

    path "/providers/data/scrape" >=> xcookie (fun ck ctx -> async {
      let url = ck.["url"]
      let csv = WebScrape.DataProviders.getCSVTree url
      let! upload = Storage.Cache.uploadFile url (csv.SaveToString())
      match upload with 
      | Choice2Of2 msg -> return! RequestErrors.BAD_REQUEST msg ctx
      | Choice1Of2 id ->
          let sch = 
            [ Schema("http://schema.org", "WebPage", ["url", JsonValue.String url ])
              Schema("http://schema.thegamma.net", "CompletionItem", ["hidden", JsonValue.Boolean true ]) ]
          return! ctx |> Serializer.returnMembers [
            Member("preview", None, Result.Nested("/null"), [], sch)
            Member("explore", None, Result.Provider("pivot", root + "/providers/data/query/" + id), [], [])
          ] })
      
  ]

