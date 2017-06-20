module WebScrape.DataProviders

open System
open FSharp.Data

type ParsingContext = 
    { Headings : Map<string, string> 
      Rows : list<string[]> }

type Demo = CsvProvider<"Heading 2, Heading 3, Item">

let getCSVTree url =
  let page = Http.RequestString(url, responseEncodingOverride="UTF-8")
  //let page = "/Users/myong/Documents/workspace/thegamma-wiki/data/2015.html"
  let doc = HtmlDocument.Parse(page)

  let specialHeadings = set ["h2";"h3"]
  let leafElements = set ["li"]

  let rec visitElement ctx (el:HtmlNode) =
    let name = el.Name()
    if specialHeadings.Contains name then
      { ctx with Headings = Map.add name (el.InnerText()) ctx.Headings }
    elif leafElements.Contains name then
      let row = [ for head in specialHeadings -> defaultArg (ctx.Headings.TryFind(head)) "" ]
      let row = row @ [ el.InnerText() ] |> Array.ofList
      { ctx with Rows = row::ctx.Rows }
    else
      el.Elements() |> Seq.fold visitElement ctx

  let emptyCtx = { Headings = Map.empty; Rows = [] }
  let resCtx = doc.Elements() |> Seq.fold visitElement emptyCtx

  let csv = 
    [ for row in resCtx.Rows -> Demo.Row(row.[0], row.[1], row.[2]) ]
    |> Demo.GetSample().Append

  csv.Save("/Users/myong/Documents/workspace/thegamma-wiki/data/2015_Parsed.csv")
  csv
// getCSVTree "https://en.wikipedia.org/wiki/2015_in_the_United_Kingdom"