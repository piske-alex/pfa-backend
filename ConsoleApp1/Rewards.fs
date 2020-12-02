module Rewards
open System
open Npgsql.FSharp
open Extreme.Mathematics

let defaultConnection  =
    Npgsql.FSharp.Sql.host "--"
    |> Npgsql.FSharp.Sql.port 1433
    |> Npgsql.FSharp.Sql.username "--"
    |> Npgsql.FSharp.Sql.password "P@--"
    |> Npgsql.FSharp.Sql.database "--"
    |> Npgsql.FSharp.Sql.sslMode Npgsql.FSharp.SslMode.Disable
    |> Npgsql.FSharp.Sql.config "Pooling=true"

let resolveOption (x:string option) = match x with
                                        | Some(x) -> x
                                        | None -> "None"

let resolveOptionGuid (x:Guid option) = match x with
                                           | Some(x) -> x
                                           | None -> Guid.Empty

let resolveResult (x: Result<int, exn>) = match x with
                                            | Ok req -> Console.WriteLine (string req) 
                                            | Error e -> printfn "Error: %s" e.Message

type User = {
    Address: string
    UserName: string
    ReferredBy: string option // notice option here
}

let getUser x : Result<User list, exn> =
    defaultConnection
    |> Npgsql.FSharp.Sql.connectFromConfig
    |> Npgsql.FSharp.Sql.query "SELECT * FROM users WHERE address = @address"
    |> Npgsql.FSharp.Sql.parameters [ "address", Npgsql.FSharp.Sql.string x ]
    |> Npgsql.FSharp.Sql.execute (fun read ->
        {
            Address = read.text "address"
            UserName = read.text "username"
            ReferredBy = read.textOrNone "referredBy" // reading nullable column
        })

let getUserReferredBy x : Result<User list, exn> =
    defaultConnection
    |> Npgsql.FSharp.Sql.connectFromConfig
    |> Npgsql.FSharp.Sql.query """WITH  RECURSIVE  q AS 
                                (
                                SELECT  *
                                FROM    users
                                WHERE   "address" = @address
                                UNION ALL
                                SELECT  m.*
                                FROM    users m
                                JOIN    q
                                ON      m.address = q."referredBy"
                                )
                                SELECT  *
                                FROM    q"""
    |> Npgsql.FSharp.Sql.parameters [ "address", Npgsql.FSharp.Sql.string x ]
    |> Npgsql.FSharp.Sql.execute (fun read ->
        {
            Address = read.text "address"
            UserName = read.text "username"
            ReferredBy = read.textOrNone "referredBy" // reading nullable column
        })
    
let getUserReferrals x : Result<User list, exn> =
    defaultConnection
    |> Npgsql.FSharp.Sql.connectFromConfig
    |> Npgsql.FSharp.Sql.query "SELECT * FROM users WHERE address = @address"
    |> Npgsql.FSharp.Sql.parameters [ "address", Npgsql.FSharp.Sql.string x ]
    |> Npgsql.FSharp.Sql.execute (fun read ->
        {
            Address = read.text "address"
            UserName = read.text "username"
            ReferredBy = read.textOrNone "referredBy" // reading nullable column
        })
    
Console.WriteLine (getUserReferredBy "0xd788fab65c3b27fed61806b4df717681d4e3c30b")

type Curve = {
    Scenario: string
    Percentage: Double
}

let getCurve x : Result<Curve list, exn> =
    defaultConnection
    |> Npgsql.FSharp.Sql.connectFromConfig
    |> Npgsql.FSharp.Sql.query "SELECT * FROM curve WHERE remark = @remark"
    |> Npgsql.FSharp.Sql.parameters [ "remark", Npgsql.FSharp.Sql.string x ]
    |> Npgsql.FSharp.Sql.execute (fun read ->
        {
            Scenario = read.text "remark"
            Percentage = read.double "percentage"
        })
    
getCurve "Video maker"
    
let getRealCurve(x:Result<Curve list, exn>) = match x with
                                                | Ok req -> (req.[0].Percentage)

type Repost = {
    UserAddress: string
    Id: Guid
}

let getRepostHierachy (x:string) : Result<Repost list, exn> =
    defaultConnection
    |> Npgsql.FSharp.Sql.connectFromConfig
    |> Npgsql.FSharp.Sql.query """WITH  RECURSIVE  q AS 
                                (
                                SELECT  *
                                FROM    reposts
                                WHERE   "id" = @id
                                UNION ALL
                                SELECT  m.*
                                FROM    reposts m
                                JOIN    q
                                ON      m.id = q."referredId"
                                )
                                SELECT  *
                                FROM    q"""
    |> Npgsql.FSharp.Sql.parameters [ "id", Npgsql.FSharp.Sql.uuid (x |> Guid) ]
    |> Npgsql.FSharp.Sql.execute (fun read ->
        {
            UserAddress = read.text "userAddress"
            Id = read.uuid "id"
        })
    
let getRealReposts(x:Result<Repost list, exn>) = match x with
                                                    | Ok req -> (req)
    
getRepostHierachy "9361cd43-edab-4d4f-9aac-e36ef08b385a"


type Order = {
    BuyerUserAddress: string
    StoryUserAddress: string
    ReferrerUserAddress: string option
    ReferrerPostId: Guid option
    SellerAddress: string 
    ProductShare: Double 
    ReferrerLevel: Int64 option
    Price: string
    Quantity: Int64
    Paid: bool
    StoryId: Guid
    ItemId: Guid
    BuyerRegistrationReferrerAddress: string option
}

let getOrder (x:string) : Result<Order list, exn> =
    defaultConnection
    |> Npgsql.FSharp.Sql.connectFromConfig
    |> Npgsql.FSharp.Sql.query """ SELECT users."referredBy" as "buyerRegistrationReferrer", items.id as "itemId", items."storyId", products."ownerAddress" as selleraddress, products.share as productshare, level, items.price, quantity, stories."userAddress" as storyUserAddress, reposts."userAddress" as referrerUserAddress, orders."userAddress" as buyerUserAddress, paid, "referralId" FROM items left join reposts on items."referralId" = reposts.id left join orders on items."orderId" = orders.id left join stories on items."storyId" = stories.id left join products on items."productId" = products.id left join users on orders."userAddress" = users.address where orders.id = @id """
    |> Npgsql.FSharp.Sql.parameters [ "id", Npgsql.FSharp.Sql.uuid (x |> Guid) ]
    |> Npgsql.FSharp.Sql.execute (fun read ->
        {
            BuyerUserAddress = read.string "buyeruseraddress"
            ReferrerUserAddress = read.stringOrNone "referreruseraddress"
            StoryUserAddress = read.string "storyuseraddress"
            Price = read.string "price"
            Quantity = read.int64 "quantity"
            ProductShare = read.double "productshare"
            SellerAddress = read.string "selleraddress"
            Paid = read.bool "paid"
            ReferrerLevel = read.int64OrNone "level"
            ReferrerPostId = read.uuidOrNone "referralId"
            StoryId = read.uuid "storyId"
            ItemId = read.uuid "itemId"
            BuyerRegistrationReferrerAddress = read.stringOrNone "buyerRegistrationReferrer"
        })
    
getOrder "fc70e5ac-5a43-496f-895d-10961a1127ea"

let setRepostReward (y:Guid) (z:string) (a:Guid) (b:Guid) (c:string) (x:string): Result<int, exn>  =
    defaultConnection
    |> Npgsql.FSharp.Sql.connectFromConfig
    |> Npgsql.FSharp.Sql.query """Insert into rewards
                                  (amount,"itemId","agentAddress","repostId","storyId","remark")
                                  VALUES (@amount, @itemId, @agentAddress, @repostId, @storyId, @remark)  """
    |> Npgsql.FSharp.Sql.parameters [ ("amount", Npgsql.FSharp.Sql.string x); ( "itemId", Npgsql.FSharp.Sql.uuid (y) );
                                    ("agentAddress", Npgsql.FSharp.Sql.string z); ("repostId", Npgsql.FSharp.Sql.uuid ( a));
                                    ("storyId", Npgsql.FSharp.Sql.uuid (b)); ("remark", Npgsql.FSharp.Sql.string c)]
    |> Npgsql.FSharp.Sql.executeNonQuery
    
let setReward (y:Guid) (z:string) (b:Guid) (c:string) (x:string): Result<int, exn>  =
    defaultConnection
    |> Npgsql.FSharp.Sql.connectFromConfig
    |> Npgsql.FSharp.Sql.query """Insert into rewards
                                  (amount,"itemId","agentAddress","storyId","remark")
                                  VALUES (@amount, @itemId, @agentAddress, @storyId, @remark)  """
    |> Npgsql.FSharp.Sql.parameters [ ("amount", Npgsql.FSharp.Sql.string x); ( "itemId", Npgsql.FSharp.Sql.uuid (y) );
                                    ("agentAddress", Npgsql.FSharp.Sql.string z); 
                                    ("storyId", Npgsql.FSharp.Sql.uuid (b)); ("remark", Npgsql.FSharp.Sql.string c)]
    |> Npgsql.FSharp.Sql.executeNonQuery

type SumResult = {
    Sum: Double option
}

let getUserTotalSpent (x:string) : Result<SumResult list, exn> =
    defaultConnection
    |> Npgsql.FSharp.Sql.connectFromConfig
    |> Npgsql.FSharp.Sql.query """SELECT SUM(CAST(SUBSTR(items.price, 0, LENGTH(items.price) - 14) as bigint) / 1000.0) as sum
                                    FROM public.orders inner join items on items."orderId" = orders.id 
                                    WHERE "userAddress" = @userAddress AND paid = true"""
    |> Npgsql.FSharp.Sql.parameters [ "userAddress", Npgsql.FSharp.Sql.string (x) ]
    |> Npgsql.FSharp.Sql.execute (fun read ->
        {
           Sum = read.doubleOrNone "sum"
        })

let getUserReferralTotalReward x: Result<SumResult list, exn> =
    defaultConnection
    |> Npgsql.FSharp.Sql.connectFromConfig
    |> Npgsql.FSharp.Sql.query """WITH  RECURSIVE  q AS 
                                    (
                                    SELECT  *
                                    FROM    users
                                    WHERE   "referredBy" = @userAddress
                                    UNION ALL
                                    SELECT  m.*
                                    FROM    users m
                                    JOIN    q
                                    ON      m."referredBy" = q."address"
                                    )
                                    SELECT  SUM(CAST(SUBSTR(rewards.amount, 0, LENGTH(rewards.amount) - 14) as bigint) / 1000.0)
                                    FROM q left join rewards on q.address = rewards."agentAddress" """
    |> Npgsql.FSharp.Sql.parameters [ "userAddress", Npgsql.FSharp.Sql.string (x) ]
    |> Npgsql.FSharp.Sql.execute (fun read ->
        {
           Sum = read.doubleOrNone "sum"
        })


    
let resolveSums (x: Result<SumResult list, exn>) :Double = 
    match x with 
    |Ok req -> (match req.[0].Sum with
                |Some m -> m
                |None -> 0.0)
    |Error e -> 0.0

resolveSums (getUserTotalSpent "0x73d912358d0f00c4767bceaeecc8fd333db9a7a1")

type VIPPolicy = {
    Value: Double
    Policy: Double
    Name: string
}

let getVIPPolicy (x:double) : Result<VIPPolicy list, exn> =
    defaultConnection
    |> Npgsql.FSharp.Sql.connectFromConfig
    |> Npgsql.FSharp.Sql.query """SELECT * FROM public.constants WHERE key LIKE 'VIP%' 
                                    AND policy <= @userPurchaseTotal order by policy desc LIMIT 1;"""
    |> Npgsql.FSharp.Sql.parameters [ "userPurchaseTotal", Npgsql.FSharp.Sql.double (x) ]
    |> Npgsql.FSharp.Sql.execute (fun read ->
        {
           Value = Double.Parse(read.string "value")
           Policy = read.double "policy"
           Name = read.string "key"
        })
    
let getCommunityPolicy (x:double) : Result<VIPPolicy list, exn> =
    defaultConnection
    |> Npgsql.FSharp.Sql.connectFromConfig
    |> Npgsql.FSharp.Sql.query """SELECT * FROM public.constants WHERE key NOT LIKE 'VIP%' 
                                    AND policy <= @userReferralPurchaseTotal order by policy desc LIMIT 1;"""
    |> Npgsql.FSharp.Sql.parameters [ "userReferralPurchaseTotal", Npgsql.FSharp.Sql.double (x) ]
    |> Npgsql.FSharp.Sql.execute (fun read ->
        {
           Value = Double.Parse(read.string "value")
           Policy = read.double "policy"
           Name = read.string "key"
        })

let videoMakerReward = getRealCurve("Video maker" |> getCurve) /100.0
let directReferralReward = getRealCurve("Repost without referral" |> getCurve) /100.0
let directSalesReward = getRealCurve("Repost direct sales" |> getCurve) /100.0
let poolReward x = getRealCurve("Referral pool" |> getCurve) /100.0 /x
    
let ethSuffix = new BigRational(BigInteger.Pow(new BigInteger(10),18))
let ethSuffixFloat = BigFloat.Pow(new BigFloat(10),18)
let inBigInt (price:string) = BigInteger.Parse(price)
let priceInDouble(order: Order) = 
    let x = BigRational.Divide( (new BigRational(inBigInt(order.Price))) , ethSuffix )
    (double x) * (double 1)
  
let timesReward (sharePercent, reward, price) :string = 
    if BigFloat.Round(BigFloat.Multiply(new BigFloat((sharePercent * reward * price)|>float),ethSuffixFloat)).IsZero then
        "0000000000000000000"
    else BigFloat.Round(BigFloat.Multiply(new BigFloat((sharePercent * reward * price)|>float),ethSuffixFloat)).ToString().Split('.').[0]
    
let reposts x = getRealReposts(x |> getRepostHierachy)

let getRegisterReferralReward (order:Order) = 
    match order.BuyerRegistrationReferrerAddress with
    |Some x -> (timesReward(1.0, 0.01, priceInDouble(order)) |> string |>
                                           (setReward order.ItemId x order.StoryId "user registration referrer") |> resolveResult)
    |None -> ignore()

let getResellReward (order:Order) = 
    let sharePercent = order.ProductShare / 100.0
    match order.ReferrerLevel with
    | Some(x) -> (x |> fun y ->
                 if (int y)>0 then 
                     match order.ReferrerPostId with
                     | Some(x) -> (reposts(x |> string) |> 
                            fun(reposts) -> ( 
                                for repost in reposts do
                                    if repost.Id.Equals(x) then
                                        (timesReward(sharePercent, directSalesReward, priceInDouble(order) ) |> string |>
                                        (setRepostReward order.ItemId repost.UserAddress repost.Id order.StoryId "pay to repost") |> resolveResult)
                                    else
                                        (timesReward(sharePercent, poolReward(double y), priceInDouble(order) ) |>  string |>
                                        (setRepostReward order.ItemId repost.UserAddress repost.Id order.StoryId "pay to repost direct") |> resolveResult)))
                 else 
                      timesReward(sharePercent, directReferralReward, priceInDouble(order) ) |> string |>
                      (setRepostReward order.ItemId (resolveOption order.ReferrerUserAddress) (resolveOptionGuid order.ReferrerPostId) order.StoryId "pay to repost direct sole") |> resolveResult
                 (timesReward(sharePercent, videoMakerReward, priceInDouble(order) ) |> string) |> (setReward order.ItemId order.StoryUserAddress order.StoryId "pay to video maker" ) |> resolveResult     
                 )
    | None ->  (timesReward(sharePercent, videoMakerReward + directReferralReward, priceInDouble(order) ) |> string) |> (setReward order.ItemId order.StoryUserAddress order.StoryId "pay to video maker" ) |> resolveResult




let getCommunityReward (order:Order)=
    let mutable lastUserPolicyAmount = 0.0
    let mutable lastShare = 0.0
    order.BuyerUserAddress |> getUserReferralTotalReward |> resolveSums |> getCommunityPolicy |> fun (x:Result<VIPPolicy list,exn>) -> (
        match x with
        |Ok req -> req |> fun (req:VIPPolicy list) -> ((lastUserPolicyAmount <- req.[0].Value); (lastShare <- System.Math.Max(req.[0].Value-15.0,0.0)); (timesReward(order.ProductShare / 100.0, System.Math.Max(req.[0].Value-15.0,0.0) / 100.0, priceInDouble(order) )))
        |Error e -> "0000000000000000000"
    ) |> (setReward order.ItemId order.BuyerUserAddress order.StoryId "pay to Buyer Community himself reward" ) |> resolveResult
    order.BuyerUserAddress |> getUserReferredBy |> fun (x:Result<User list,exn>) -> (match x with
                                                     |Ok req -> (for senior in req do 
                                                        Console.WriteLine senior
                                                        senior.Address |> getUserReferralTotalReward |> resolveSums |>
                                                                 getCommunityPolicy |> fun (x:Result<VIPPolicy list,exn>) -> (
                                                                 match x with
                                                                 |Ok req -> (if (req.[0].Value > lastUserPolicyAmount) then (
                                                                                (timesReward(order.ProductShare / 100.0, (System.Math.Max(req.[0].Value-15.0,0.0) - lastShare) / 100.0, priceInDouble(order) ), req.[0])
                                                                             )
                                                                             else ("0000000000000000000", req.[0])
                                                                            )
                                                                 |Error e -> ("0000000000000000000", { Value= 0.00; Policy= 0.00; Name= "Errored"})
                                                                 )|> (fun (x:string, req:VIPPolicy) -> (lastUserPolicyAmount <- req.Value;
                                                                               lastShare <- System.Math.Max(req.Value-15.0,0.0);
                                                                               x)) |> (setReward order.ItemId senior.Address order.StoryId "pay to Buyer Community friends reward" ) |> resolveResult
                                                                 ) 
                                                     |Error e -> (Console.WriteLine e.Message))

    
let getPurchaseReward (order:Order)=
    order.BuyerUserAddress |> getUserTotalSpent |> resolveSums |> 
        getVIPPolicy |> fun (x:Result<VIPPolicy list,exn>) -> (match x with
                                                                |Ok req -> (timesReward(order.ProductShare / 100.0, req.[0].Value / 100.0, priceInDouble(order) ))
                                                                |Error e -> "0000000000000000000") |>
        (setReward order.ItemId order.BuyerUserAddress order.StoryId "pay to Buyer" ) |> resolveResult

let testOrder = (getOrder "fc70e5ac-5a43-496f-895d-10961a1127ea") 

let loopThroughItems (orders:Order list) = 
    for order in orders do
         order |> getResellReward
         order |> getPurchaseReward 
         order |> getCommunityReward 
         order |> getRegisterReferralReward
    
let getReward (x:Result<Order list, exn>) = match x with
    | Ok req -> (req |> loopThroughItems)

getReward testOrder
