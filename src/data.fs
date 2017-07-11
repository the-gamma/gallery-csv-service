module Gallery.CsvService.DataProviders

open System
open Suave
open Suave.Filters
open Suave.Operators
open Gallery.CsvService
open Gallery.CsvService.Storage
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
        Member("loadTable", Some [Parameter("url", Type.Named("string"), false, ParameterKind.Static("url"))], Result.Nested("/upload"), [])
        // Member("scrape", Some [Parameter("url", Type.Named("string"), false, ParameterKind.Static("url"))], Result.Nested("/scrapeFrom"), [])
        Member("scrapeLists", Some [Parameter("url", Type.Named("string"), false, ParameterKind.Static("url"))], Result.Nested("/getAllEntries"), [])
        Member("scrapeDatedLists", Some [Parameter("url", Type.Named("string"), false, ParameterKind.Static("url"))], Result.Nested("/getDatedEntries"), [])
      ])

    path "/providers/data/upload" >=> xcookie (fun ck ctx -> async {
      use wc = new System.Net.WebClient()
      let url = ck.["url"]
      let! file = wc.AsyncDownloadString(Uri(url))
      let! upload = Storage.Cache.uploadFile url file "uploadedCSV"
      match upload with 
      | Choice2Of2 msg -> return! RequestErrors.BAD_REQUEST msg ctx
      | Choice1Of2 id ->
          return! ctx |> Serializer.returnMembers [
            Member("explore", None, Result.Provider("pivot", root + "/providers/data/query/" + id), [])
          ] })

    pathScan "/providers/data/query/%s" (fun id ctx -> async {
      let! file = Storage.Cache.fetchFile id
      printfn "File: %A" file
      match file with 
      | None ->
          return! RequestErrors.BAD_REQUEST "File has not been uploaded." ctx
      | Some(meta, data) ->
          return! Pivot.handleRequest meta data (List.map fst ctx.request.query) ctx }
    )

    path "/providers/data/scrapeFrom" >=> request (fun r -> 
      Serializer.returnMembers [
        Member("allLists", Some [Parameter("url", Type.Named("string"), false, ParameterKind.Static("url"))], Result.Nested("/getAllEntries"), [])
        Member("datedLists", Some [Parameter("url", Type.Named("string"), false, ParameterKind.Static("url"))], Result.Nested("/getDatedEntries"), [])
      ])

    path "/providers/data/getAllEntries" >=> xcookie (fun ck ctx -> async {
      let url = ck.["url"]
      let csv = WebScrape.DataProviders.getAllEntries url
      let! upload = Storage.Cache.uploadFile url (csv.SaveToString()) "allEntries"
      match upload with 
      | Choice2Of2 msg -> return! RequestErrors.BAD_REQUEST msg ctx
      | Choice1Of2 id ->
          return! ctx |> Serializer.returnMembers [
            Member("explore", None, Result.Provider("pivot", root + "/providers/data/query/" + id), [])
          ] })
    
    path "/providers/data/getDatedEntries" >=> xcookie (fun ck ctx -> async {
      let url = ck.["url"]
      printfn "URL: %s" url
      let csv = WebScrape.DataProviders.getDatedEntries url
      let! upload = Storage.Cache.uploadFile url (csv.SaveToString()) "datedEntries"
      printfn "upload: %A" upload
      match upload with 
      | Choice2Of2 msg -> return! RequestErrors.BAD_REQUEST msg ctx
      | Choice1Of2 id ->
          return! ctx |> Serializer.returnMembers [
            Member("explore", None, Result.Provider("pivot", root + "/providers/data/query/" + id), [])
          ] })
    
    
    
    // Member("explore", None, Result.Nested("pivot", root + "/providers/data/query/" + id), [])
      
  ]

