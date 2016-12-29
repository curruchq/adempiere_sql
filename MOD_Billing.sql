create or replace
PACKAGE             "MOD_BILLING" IS

PROCEDURE getduedate(
 p_client_id                 IN NUMBER,
 p_org_id                    IN NUMBER,
 p_paymentterm_id            IN NUMBER,
 p_cycle_date                IN DATE,
 p_run_as_date               IN DATE,
 p_duedate                   OUT DATE
);

PROCEDURE submit_rating(
 p_param_id                  NUMBER
);

PROCEDURE rating(
 p_debug                     NUMBER,
 p_client_id                 NUMBER,
 p_org_id                    NUMBER
);

PROCEDURE submit(
 p_param_id                  NUMBER
);

PROCEDURE main(
 p_debug                     NUMBER,
 p_client_id                 NUMBER,
 p_org_id                    NUMBER,
 p_run_as_date               DATE, -- Billing run date. ie make bills for that biling schedule.
 P_Bpartner_Id               Number Default Null,
 p_bill_calls                VARCHAR2,
 p_apply_discount            VARCHAR2
);

END mod_billing;
/


create or replace
PACKAGE BODY             "MOD_BILLING" IS

--================================================================================================
-- History
--   1.0.0 01-JUN-08 Created       
--   1.3.0 08-AUG-08 Changed line total amount to be sum of non-tax amount. 
--                   Add bill_qty to main - Multiplier for Qty
--                   Remove ad_user_id from header insert
--                   Add logic for subtype OneOff
--   1.3.1 14-AUG-08 Change getprice to use listprice rather than stdprice
--   1.3.2 23-AUG-08 Alter tax code default logic.
--   1.3.3 08-OCT-08 Change getprice to use stdprice not listprice
--   1.3.4 14-OCT-08 Clear tax id if exempt.
--   2.0.0 08-OCT-08 Add call billing logic
--   2.0.5 19-JAN-09 Alter submit logging 
--   2.0.6 14-MAR-09 Alter logic of pay thru date
--   2.0.7 14-DEC-16 Add discount calculation logic
--================================================================================================

g_trapped                    EXCEPTION;
g_debug                      NUMBER := 3;
g_batch_id                   NUMBER;
g_ref                        NUMBER;
g_err                        NUMBER;
g_errm                       VARCHAR2(1000);             
g_invoice_count              NUMBER;
g_invoice_line_count         NUMBER;

--================================================================================================
-- Get a price for an item and a date form a specific price list
--================================================================================================

PROCEDURE getprice(
 p_client_id                 IN NUMBER,
 p_org_id                    IN NUMBER,
 p_pricelist_id              IN NUMBER,
 p_product_id                IN NUMBER,
 p_price_date                IN DATE,
 p_getpricestd               OUT NUMBER,
 p_getpricelist              OUT NUMBER,
 p_getpricelimit             OUT NUMBER
)
IS

v_resultstd                  NUMBER;
v_resultlist                 NUMBER;
v_resultlimit                NUMBER;
v_pricelistversion_id        NUMBER;
v_baseversion_id             NUMBER;

BEGIN

-- Get latest price list verison
BEGIN
     SELECT v1.m_pricelist_version_id,
            v1.m_pricelist_version_base_id
     INTO   v_pricelistversion_id,
            v_baseversion_id
     FROM   m_pricelist_version v1
     WHERE  v1.m_pricelist_id          = p_pricelist_id
     AND    v1.isactive                = 'Y'
     AND    v1.validfrom               = 
            (SELECT MAX(v2.validfrom)
             FROM   m_pricelist_version v2
             WHERE  v2.m_pricelist_id  = v1.m_pricelist_id
             AND    v2.isactive        = 'Y'
             AND    v2.validfrom       <= p_price_date);
EXCEPTION
WHEN NO_DATA_FOUND THEN
     mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,2,'P','mod_billing.getprice.version','NODATA:'||p_pricelist_id||':'||p_product_id||':'||p_price_date);
     v_resultstd := NULL;
     v_resultlist := NULL;
     v_resultlimit := NULL;
     RAISE g_trapped;
WHEN TOO_MANY_ROWS THEN
     mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,2,'P','mod_billing.getprice.version','TOOMANY:'||p_pricelist_id||':'||p_product_id||':'||p_price_date);
     v_resultstd := NULL;
     v_resultlist := NULL;
     v_resultlimit := NULL;
     RAISE g_trapped;
END;

-- Get item price from current version
BEGIN
     SELECT p.pricestd, p.pricelist, p.pricelimit
     INTO   v_resultstd, v_resultlist, v_resultlimit
     FROM   m_productprice p
     WHERE  p.m_pricelist_version_id   = v_pricelistversion_id
     AND    p.m_product_id             = p_product_id
     AND    p.isactive                 = 'Y';
EXCEPTION 
WHEN NO_DATA_FOUND THEN
     v_resultstd := NULL;
     v_resultlist := NULL;
     v_resultlimit := NULL;
WHEN TOO_MANY_ROWS THEN
     mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,2,'P','mod_billing.getprice.price','TOOMANY:'||p_pricelist_id||':'||p_product_id||':'||p_price_date);
     v_resultstd := NULL;
     v_resultlist := NULL;
     v_resultlimit := NULL;
     RAISE g_trapped;
END;

-- If none found so far try the base price list
IF v_resultstd IS NULL THEN

   BEGIN
     SELECT p.pricestd, p.pricelist, p.pricelimit
     INTO   v_resultstd, v_resultlist, v_resultlimit
     FROM   m_productprice p
     WHERE  p.m_pricelist_version_id   = v_baseversion_id
     AND    p.m_product_id             = p_product_id
     AND    p.isactive                 = 'Y';
   EXCEPTION
   WHEN NO_DATA_FOUND THEN
     mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,2,'P','mod_billing.getprice.base','NODATA:'||p_pricelist_id||':'||p_product_id||':'||p_price_date);
     v_resultstd := NULL;
     v_resultlist := NULL;
     v_resultlimit := NULL;
     RAISE g_trapped;
   WHEN TOO_MANY_ROWS THEN
     mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,2,'P','mod_billing.getprice.base','TOOMANY:'||p_pricelist_id||':'||p_product_id||':'||p_price_date);
     v_resultstd := NULL;
     v_resultlist := NULL;
     v_resultlimit := NULL;
     RAISE g_trapped;
   END;
   
END IF;

p_getpricestd      := v_resultstd;
p_getpricelist     := v_resultlist;
p_getpricelimit    := v_resultlimit;

EXCEPTION 
WHEN OTHERS THEN
     g_err := SQLCODE;
     g_errm := SQLERRM;
     mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,1,'P','mod_billing.getprice','UNKNOWN:'||g_err||':'||g_errm||':'||p_pricelist_id||':'||p_product_id||':'||p_price_date);
     RAISE;

END getprice;

--================================================================================================
-- Get a discounted price for an item and a date form a specific price list
--================================================================================================

PROCEDURE getdiscountedprice(
 p_client_id                 IN NUMBER,
 p_org_id                    IN NUMBER,
 p_pricelist_id              IN NUMBER,
 p_product_id                IN NUMBER,
 p_price_date                IN DATE,
 p_bpartner_id               IN NUMBER,
 p_quantity                  IN NUMBER,
 p_getpricestd               OUT NUMBER,
 p_getpricelist              OUT NUMBER,
 p_getpricelimit             OUT NUMBER
)
IS

v_resultstd                  NUMBER;
v_resultlist                 NUMBER;
v_resultlimit                NUMBER;
v_pricelistversion_id        NUMBER;
v_baseversion_id             NUMBER;
v_discountschema_id          NUMBER;
v_discount		     NUMBER;
v_discountedprice            NUMBER;

-- Select all discount breaks for a product 
CURSOR breaks_c (i_m_discountschema_id NUMBER)  IS
SELECT dsb.*
FROM   m_discountschemabreak dsb
WHERE  dsb.m_discountschema_id = i_m_discountschema_id AND dsb.m_product_id = p_product_id
ORDER BY dsb.seqno;

BEGIN


-- Get latest price list verison
BEGIN
     SELECT v1.m_pricelist_version_id,
            v1.m_pricelist_version_base_id
     INTO   v_pricelistversion_id,
            v_baseversion_id
     FROM   m_pricelist_version v1
     WHERE  v1.m_pricelist_id          = p_pricelist_id
     AND    v1.isactive                = 'Y'
     AND    v1.validfrom               = 
            (SELECT MAX(v2.validfrom)
             FROM   m_pricelist_version v2
             WHERE  v2.m_pricelist_id  = v1.m_pricelist_id
             AND    v2.isactive        = 'Y'
             AND    v2.validfrom       <= p_price_date);
EXCEPTION
WHEN NO_DATA_FOUND THEN
     mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,2,'P','mod_billing.getprice.version','NODATA:'||p_pricelist_id||':'||p_product_id||':'||p_price_date);
     v_resultstd := NULL;
     v_resultlist := NULL;
     v_resultlimit := NULL;
     RAISE g_trapped;
WHEN TOO_MANY_ROWS THEN
     mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,2,'P','mod_billing.getprice.version','TOOMANY:'||p_pricelist_id||':'||p_product_id||':'||p_price_date);
     v_resultstd := NULL;
     v_resultlist := NULL;
     v_resultlimit := NULL;
     RAISE g_trapped;
END;

-- Get item price from current version
BEGIN
     SELECT p.pricestd, p.pricelist, p.pricelimit
     INTO   v_resultstd, v_resultlist, v_resultlimit
     FROM   m_productprice p
     WHERE  p.m_pricelist_version_id   = v_pricelistversion_id
     AND    p.m_product_id             = p_product_id
     AND    p.isactive                 = 'Y';
EXCEPTION 
WHEN NO_DATA_FOUND THEN
     v_resultstd := NULL;
     v_resultlist := NULL;
     v_resultlimit := NULL;
WHEN TOO_MANY_ROWS THEN
     mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,2,'P','mod_billing.getprice.price','TOOMANY:'||p_pricelist_id||':'||p_product_id||':'||p_price_date);
     v_resultstd := NULL;
     v_resultlist := NULL;
     v_resultlimit := NULL;
     RAISE g_trapped;
END;

-- If none found so far try the base price list
IF v_resultstd IS NULL THEN

   BEGIN
     SELECT p.pricestd, p.pricelist, p.pricelimit
     INTO   v_resultstd, v_resultlist, v_resultlimit
     FROM   m_productprice p
     WHERE  p.m_pricelist_version_id   = v_baseversion_id
     AND    p.m_product_id             = p_product_id
     AND    p.isactive                 = 'Y';
   EXCEPTION
   WHEN NO_DATA_FOUND THEN
     mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,2,'P','mod_billing.getprice.base','NODATA:'||p_pricelist_id||':'||p_product_id||':'||p_price_date);
     v_resultstd := NULL;
     v_resultlist := NULL;
     v_resultlimit := NULL;
     RAISE g_trapped;
   WHEN TOO_MANY_ROWS THEN
     mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,2,'P','mod_billing.getprice.base','TOOMANY:'||p_pricelist_id||':'||p_product_id||':'||p_price_date);
     v_resultstd := NULL;
     v_resultlist := NULL;
     v_resultlimit := NULL;
     RAISE g_trapped;
   END;
   
END IF;

-- Get discount schema from business partner
BEGIN
     SELECT bp.m_discountschema_id
     INTO   v_discountschema_id
     FROM   c_bpartner bp
     WHERE bp.c_bpartner_id = p_bpartner_id;
EXCEPTION 
WHEN NO_DATA_FOUND THEN
     v_discountschema_id := NULL;
WHEN TOO_MANY_ROWS THEN
     mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,2,'P','mod_billing.getdiscountprice.discountschema','TOOMANY:'||p_bpartner_id);
     v_discountschema_id := NULL;
     RAISE g_trapped;
END;

v_discount := 0;
FOR break IN breaks_c (v_discountschema_id) LOOP
	IF (p_quantity > break.breakvalue) THEN
		v_discount := break.breakdiscount;
	END IF ;
END LOOP;

v_discountedprice := (100-v_discount)/100;

p_getpricestd      := ROUND(v_resultstd * v_discountedprice , 2);
p_getpricelist     := v_resultlist;
p_getpricelimit    := v_resultlimit;

EXCEPTION 
WHEN OTHERS THEN
     g_err := SQLCODE;
     g_errm := SQLERRM;
     mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,1,'P','mod_billing.getprice','UNKNOWN:'||g_err||':'||g_errm||':'||p_pricelist_id||':'||p_product_id||':'||p_price_date);
     RAISE;

END getdiscountedprice;

--================================================================================================
-- Work out the due date
--================================================================================================

PROCEDURE getduedate(
 p_client_id                 IN NUMBER,
 p_org_id                    IN NUMBER,
 p_paymentterm_id            IN NUMBER,
 p_cycle_date                IN DATE,
 p_run_as_date               IN DATE,
 p_duedate                   OUT DATE
)
IS

v_duedate                    DATE;
v_isduefixed                 VARCHAR2(1);
v_fixmonthcutoff             NUMBER;
v_fixmonthday                NUMBER;
v_fixmonthoffset             NUMBER;
v_netdays                    NUMBER;
v_netday                     NUMBER;

BEGIN

-- Get payment term details
BEGIN
     SELECT t.isduefixed,
            t.fixmonthcutoff,
            t.fixmonthday,
            t.fixmonthoffset,
            t.netdays,
            t.netday
     INTO   v_isduefixed,
            v_fixmonthcutoff,
            v_fixmonthday,
            v_fixmonthoffset,
            v_netdays,
            v_netday
     FROM   c_paymentterm t
     WHERE  t.c_paymentterm_id         = p_paymentterm_id;
EXCEPTION
WHEN NO_DATA_FOUND THEN
     mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,2,'P','mod_billing.getduedate.pymtterm','NODATA:'||p_paymentterm_id);
     v_duedate := NULL;
     RAISE g_trapped;
WHEN TOO_MANY_ROWS THEN
     mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,2,'P','mod_billing.getduedate.pymtterm','TOOMANY:'||p_paymentterm_id);
     v_duedate := NULL;
     RAISE g_trapped;
END;

-- If not fixed days then add days to cycle date
IF v_isduefixed = 'N' THEN
   
   v_duedate := p_cycle_date + v_netdays;
   
   -- Roll to next day of week if option set.
   IF  v_netday IS NOT NULL
   AND (TO_CHAR(v_duedate,'D') <> v_netday) THEN
       IF    v_netday = 1 THEN v_duedate := NEXT_DAY(v_duedate,'Monday'); 
       ELSIF v_netday = 2 THEN v_duedate := NEXT_DAY(v_duedate,'Tuesday');
       ELSIF v_netday = 3 THEN v_duedate := NEXT_DAY(v_duedate,'Wednesday');
       ELSIF v_netday = 4 THEN v_duedate := NEXT_DAY(v_duedate,'Thursday');
       ELSIF v_netday = 5 THEN v_duedate := NEXT_DAY(v_duedate,'Friday');
       ELSIF v_netday = 6 THEN v_duedate := NEXT_DAY(v_duedate,'Saturday');
       ELSIF v_netday = 7 THEN v_duedate := NEXT_DAY(v_duedate,'Sunday');
       END IF;
   END IF;
   
ELSE -- Fixed day of month 

   -- Set due date to day of month plus months to jump
   v_duedate := ADD_MONTHS(TO_DATE(v_fixmonthday||'-'||TO_CHAR(p_cycle_date,'MON-RR'),'DD-MON-RR'),v_fixmonthoffset);

   -- If cut off day has passed then add 1 month   
   IF TO_NUMBER(TO_CHAR(p_cycle_date,'DD')) > v_fixmonthcutoff THEN
      v_duedate := ADD_MONTHS(v_duedate,1);
   END IF;

END IF;

-- If date is before run_date then call using run date as cycle date
IF v_duedate < p_run_as_date THEN
   getduedate(p_client_id,p_org_id,p_paymentterm_id, p_run_as_date, p_run_as_date, v_duedate);
END IF;
   
-- Set result
mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,5,'P','mod_billing.getduedate','DueDate:'||v_duedate);
p_duedate := v_duedate;

EXCEPTION 
WHEN OTHERS THEN
     g_err := SQLCODE;
     g_errm := SQLERRM;
     mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,1,'P','mod_billing.getduedate','UNKNOWN:'||g_err||':'||g_errm||':'||p_paymentterm_id||':'||p_cycle_date||':'||p_run_as_date);
     RAISE;

END getduedate;

--================================================================================================
-- Create / Find Invoice Header
--================================================================================================

PROCEDURE invheader(
 p_invoice_id                IN OUT    NUMBER,
 p_client_id                 IN        NUMBER,
 p_org_id                    IN        NUMBER,
 p_start_date                IN        DATE,
 p_end_date                  IN        DATE,
 p_cycle_date                IN        DATE,
 p_bpartner_id               IN        NUMBER,
 p_bpartner_location_id      IN        NUMBER,
 p_run_as_date               IN        DATE)
AS

v_salesrep_id                NUMBER;
v_paymentrule                VARCHAR2(30);
v_paymentterm_id             NUMBER;
v_pricelist_id               NUMBER;
v_doctype_id                 NUMBER;
v_currency_id                NUMBER;
v_conversiontype_id          NUMBER;
v_documentno                 VARCHAR2(30);
v_description                VARCHAR2(200);
v_bpartner_name              VARCHAR2(100);
v_invoicepayschedule_id      NUMBER;
v_duedate                    DATE;
v_contact_user_id            NUMBER;

BEGIN

   -- Look for existing invoice
    BEGIN
       SELECT i.c_invoice_id
       INTO   p_invoice_id
       FROM   c_invoice i
       WHERE  i.ad_client_id           = p_client_id
       AND    i.ad_org_id              = p_org_id
       AND    i.c_bpartner_id          = p_bpartner_id
       AND    i.c_bpartner_location_id = p_bpartner_location_id
       AND    i.dateinvoiced           = p_cycle_date
       AND    i.docstatus              = 'DR';
       
       RETURN;

    EXCEPTION    
    WHEN TOO_MANY_ROWS THEN
        mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,2,'P','mod_billing.InvHeader','TOOMANY:'||p_bpartner_id||':'||p_end_date);
        RAISE g_trapped;
    WHEN NO_DATA_FOUND THEN
        NULL;
    END;

    -- Insert New Invoice Header
    -- Get some data from the customer etc   
    BEGIN
   
       SELECT c.salesrep_id,
              NVL(c.paymentrule,'T') payment_rule,
              c.c_paymentterm_id,
              c.m_pricelist_id,
              c.name
       INTO   v_salesrep_id,
              v_paymentrule,
              v_paymentterm_id,
              v_pricelist_id,
              v_bpartner_name
       FROM   c_bpartner c
       WHERE  c.c_bpartner_id = p_bpartner_id;
       
    EXCEPTION 
    WHEN NO_DATA_FOUND THEN
        mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,2,'P','mod_billing.InvHeader.BPartner','NOROWS:'||p_bpartner_id);
        RAISE g_trapped;
    WHEN TOO_MANY_ROWS THEN
        mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,2,'P','mod_billing.InvHeader.BPartner','TOOMANY:'||p_bpartner_id);
        RAISE g_trapped;
    END;

    -- Select default payment term
    IF v_paymentterm_id IS NULl THEN
       BEGIN
           SELECT t.c_paymentterm_id
           INTO   v_paymentterm_id
           FROM   c_paymentterm t
           WHERE  t.isdefault     =  'Y'
           AND    t.ad_client_id  =  p_client_id
           AND    t.ad_org_id     IN (0, p_org_id);
       EXCEPTION 
       WHEN NO_DATA_FOUND THEN
           mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,2,'P','mod_billing.InvHeader.DefPymtTerm','NOROWS');
           RAISE g_trapped;
       WHEN TOO_MANY_ROWS THEN
           mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,2,'P','mod_billing.InvHeader.DefPymtTerm','TOOMANY');
           RAISE g_trapped;
       END;
    END IF;
    
    -- Select doc type
    BEGIN
   
       SELECT d.c_doctype_id
       INTO   v_doctype_id
       FROM   c_doctype d
       --WHERE  UPPER(d.name)   = 'AR INVOICE'
       WHERE  d.c_doctype_id = 1000143
       AND    d.ad_client_id  = p_client_id
       AND    d.ad_org_id     IN (0,p_org_id);
       
    EXCEPTION 
    WHEN NO_DATA_FOUND THEN
        mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,2,'P','mod_billing.InvHeader.DocType','NOROWS');
        RAISE g_trapped;
    WHEN TOO_MANY_ROWS THEN
        mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,2,'P','mod_billing.InvHeader.DocType','TOOMANY');
        RAISE g_trapped;
    END;
    
    -- Select currency id
    BEGIN
   
      -- SELECT c.c_currency_id 
     --  INTO   v_currency_id
      -- FROM   c_currency c
     --  WHERE  c.iso_code      = 'NZD'
     --  AND    c.ad_client_id  = 0
     --  AND    c.ad_org_id     = 0;
     
     SELECT c.c_currency_id
       INTO   v_currency_id
       FROM   m_pricelist c
       WHERE  c.m_pricelist_id  = v_pricelist_id ;
      
       
    EXCEPTION 
    WHEN NO_DATA_FOUND THEN
        mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,2,'P','mod_billing.InvHeader.Currency','NOROWS');
        RAISE g_trapped;
    WHEN TOO_MANY_ROWS THEN
        mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,2,'P','mod_billing.InvHeader.Currency','TOOMANY');
        RAISE g_trapped;
    END;
    
    -- Select conversion type id
    BEGIN
   
       SELECT c.c_conversiontype_id
       INTO   v_conversiontype_id
       FROM   c_conversiontype c
       WHERE  UPPER(c.name)  = 'SPOT'
       AND    c.ad_client_id = 0
       AND    c.ad_org_id    = 0;
       
    EXCEPTION 
    WHEN NO_DATA_FOUND THEN
        mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,2,'P','mod_billing.InvHeader.ConvType','NOROWS');
        RAISE g_trapped;
    WHEN TOO_MANY_ROWS THEN
        mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,2,'P','mod_billing.InvHeader.ConvType','TOOMANY');
        RAISE g_trapped;
    END;
    
    -- Get invoice number
    ad_sequence_doc('AR Invoice',p_client_id,v_documentno);
    mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,5,'P','mod_billing.InvHeader.DocNo',v_documentno);

    -- Build Invoice Description
    v_description := v_bpartner_name||' '||TO_CHAR(p_cycle_date,'DD/MM/YY');
    mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,5,'P','mod_billing.InvHeader.Descr',v_description);

    -- Get next id
    ad_sequence_next('C_Invoice',1000000,p_invoice_id);
   
    -- Get BP's first contact with Billing role
    BEGIN

    	SELECT u.ad_user_id
    	INTO   v_contact_user_id
    	FROM   ad_user u, ad_user_roles ur
    	WHERE  u.c_bpartner_id = p_bpartner_id
    	AND    u.ad_user_id = ur.ad_user_id 
    	AND    ur.ad_role_id = 1000019
    	AND    rownum <= 1;
   
     EXCEPTION 
     WHEN NO_DATA_FOUND THEN
    	-- Get BP's first contact any role
    	BEGIN

    		SELECT u.ad_user_id
    		INTO   v_contact_user_id
    		FROM   ad_user u
    		WHERE  u.c_bpartner_id = p_bpartner_id
    		AND    rownum <= 1;
   
    	 EXCEPTION 
    	 WHEN NO_DATA_FOUND THEN
    		mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,2,'P','mod_billing.InvHeader.ContactUserId','NOROWS');
    		RAISE g_trapped;
    	 END;
     END;
   
    -- Insert new Invoice Header
    INSERT INTO c_invoice(
     c_invoice_id,
     ad_client_id,
     ad_org_id,
     isactive,
     created,
     createdby,
     updated,
     updatedby,
     issotrx,
     documentno,
     docstatus,
     docaction,
     processing,
     processed,
     posted,
     c_doctype_id,
     c_doctypetarget_id,
     description,
     isapproved,
     istransferred,
     isprinted,
     salesrep_id,
     dateinvoiced,
     dateacct,
     c_bpartner_id,
     c_bpartner_location_id,
     isdiscountprinted,
     c_currency_id,
     paymentrule,
     c_paymentterm_id,
     chargeamt,
     totallines,
     grandtotal,
     m_pricelist_id,
     istaxincluded,
     ispaid,
     createfrom,
     generateto,
     sendemail,
     copyfrom,
     isselfservice,
     c_conversiontype_id,
     ispayschedulevalid,
     isindispute,
     ad_user_id)
    VALUES(
     p_invoice_id,	          -- c_invoice_id
     p_client_id,             -- ad_client_id
     p_org_id,                -- ad_org_id
     'Y',	                    -- isactive
     SYSDATE,	                -- created
     0,	                      -- createdby
     SYSDATE,	                -- updated
     0,	                      -- updatedby
     'Y',	                    -- issotrx
     v_documentno,            -- documentno
     'DR',	                  -- docstatus
     'CO',	                  -- docaction
     'N',	                    -- processing
     'N',	                    -- processed
     'N',	                    -- posted
     0,	                      -- c_doctype_id
     v_doctype_id,            -- c_doctypetarget_id
     v_description,           -- description
     'N',	                    -- isapproved
     'N',	                    -- istransferred
     'N',	                    -- isprinted
     v_salesrep_id,	          -- salesrep_id
     p_cycle_date,	          -- dateinvoiced
     p_run_as_date,	          -- dateacct
     p_bpartner_id,	          -- c_bpartner_id
     p_bpartner_location_id,  -- c_bpartner_location_id
     'N',	                    -- isdiscountprinted
     v_currency_id,	          -- c_currency_id
     v_paymentrule,	          -- paymentrule
     v_paymentterm_id,	      -- c_paymentterm_id
     0,	                      -- chargeamt
     0,	                      -- totallines
     0,	                      -- grandtotal
     v_pricelist_id,          -- m_pricelist_id
     'N',	                    -- istaxincluded
     'N',	                    -- ispaid
     'N',	                    -- createfrom
     'N',	                    -- generateto
     'N',	                    -- sendemail
     'N',	                    -- copyfrom
     'N',	                    -- isselfservice
     v_conversiontype_id,	    -- c_conversiontype_id
     'Y',	                    -- ispayschedulevalid
     'N',	                    -- isindispute
     v_contact_user_id);        -- contact

    mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,3,'P','mod_billing.InvHeader','New Header Inserted:'||v_documentno);

    -- Get due date
    getduedate(p_client_id,p_org_id,v_paymentterm_id, p_cycle_date, p_run_as_date, v_duedate);

    -- Get next id
    ad_sequence_next('C_InvoicePaySchedule',1000000,v_invoicepayschedule_id);
   
    INSERT INTO c_invoicepayschedule(
     c_invoicepayschedule_id,
     ad_client_id,
     ad_org_id,
     isactive,
     created,
     createdby,
     updated,
     updatedby,
     c_invoice_id,
     duedate,
     dueamt,
     discountdate,
     discountamt,
     isvalid,
     processing,
     processed)
    VALUES(     
     v_invoicepayschedule_id, -- c_invoicepayschedule_id,
     p_client_id,             -- ad_client_id
     p_org_id,                -- ad_org_id
     'Y',	                    -- isactive
     SYSDATE,	                -- created
     0,	                      -- createdby
     SYSDATE,	                -- updated
     0,	                      -- updatedby
     p_invoice_id,            -- c_invoice_id,
     v_duedate,               -- duedate,
     0,                       -- dueamt,
     v_duedate,               -- discountdate,
     0,                       -- discountamt,
     'Y',                     -- isvalid,
     'N',                     -- processing,
     'N');                    -- processed

    -- Add to counters
    g_invoice_count := g_invoice_count + 1;
         
EXCEPTION 
WHEN OTHERS THEN
     g_err := SQLCODE;
     g_errm := SQLERRM;
     mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,1,'P','mod_billing.invheader','UNKNOWN:'||g_err||':'||g_errm);
     RAISE;

END invheader;

--================================================================================================
-- Create / Update Invoice Tax Line
--================================================================================================

PROCEDURE invtax(
 p_invoice_id                IN OUT    NUMBER,
 p_client_id                 IN        NUMBER,
 p_org_id                    IN        NUMBER)
AS

CURSOR taxes_c IS
SELECT l.c_tax_id,
       SUM(l.linenetamt) linenetamt, 
       SUM(l.taxamt)     taxamt
FROM   c_invoiceline l
WHERE  l.c_invoice_id   = p_invoice_id
GROUP BY l.c_tax_id;

v_count                      NUMBER;

BEGIN

FOR taxes IN taxes_c LOOP

   -- Look for existing tax line
    BEGIN
       SELECT COUNT(1)
       INTO   v_count
       FROM   c_invoicetax t
       WHERE  t.ad_client_id           = p_client_id
       AND    t.ad_org_id              = p_org_id
       AND    t.c_invoice_id           = p_invoice_id
       AND    t.c_tax_id               = taxes.c_tax_id;
    EXCEPTION    
    WHEN TOO_MANY_ROWS THEN
        mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,2,'P','mod_billing.InvTax.Check','TOOMANY:'||p_invoice_id||':'||taxes.c_tax_id);
        RAISE g_trapped;
    WHEN NO_DATA_FOUND THEN
        v_count := 0;
    END;

    -- Update or insert lines
    IF v_count <> 0 THEN
  
       -- Update existing line  
       UPDATE c_invoicetax i
       SET    taxbaseamt     = taxes.linenetamt,
              taxamt         = taxes.taxamt
       WHERE  i.c_invoice_id = p_invoice_id
       AND    i.c_tax_id     = taxes.c_tax_id;
       mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,5,'P','mod_billing.InvLine','Updated Invoice Tax');
       
    ELSE

       -- Insert new tax line
       INSERT INTO c_invoicetax(
        c_tax_id,
        c_invoice_id,
        ad_client_id,
        ad_org_id,
        isactive,
        created,
        createdby,
        updated,
        updatedby,
        taxbaseamt,
        taxamt,
        processed,
        istaxincluded)
       VALUES(
        taxes.c_tax_id,           -- c_tax_id
        p_invoice_id,	            -- c_invoice_id
        p_client_id,              -- ad_client_id
        p_org_id,                 -- ad_org_id
        'Y',	                    -- isactive
        SYSDATE,	                -- created
        0,	                      -- createdby
        SYSDATE,	                -- updated
        0,	                      -- updatedby
        taxes.linenetamt,         -- taxbase
        taxes.taxamt,	            -- taxamt
        'N',	                    -- processed
        'N');	                  -- iistaxincluded
        mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,5,'P','mod_billing.InvHeader','Tax Line Inserted');

     END IF;

END LOOP;
         
EXCEPTION 
WHEN OTHERS THEN
     g_err := SQLCODE;
     g_errm := SQLERRM;
     mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,1,'P','mod_billing.invtax','UNKNOWN:'||g_err||':'||g_errm);
     RAISE;

END invtax;

--================================================================================================
-- Create / Update Invoice Tax Line
--================================================================================================

PROCEDURE invtaxV2(
 p_invoice_id                IN OUT    NUMBER,
 p_client_id                 IN        NUMBER,
 p_org_id                    IN        NUMBER)
AS

CURSOR taxes_c IS
select l.c_tax_id,sum(l.linenetamt) linenetamt, round(sum(l.linenetamt * t.rate/ 100) , 2 ) taxamt
from c_invoiceline l
inner join c_tax t on (l.c_tax_id = t.c_tax_id)
where l.c_invoice_id =  p_invoice_id
group by l.c_tax_id;

v_count                      NUMBER;

BEGIN

FOR taxes IN taxes_c LOOP

   -- Look for existing tax line
    BEGIN
       SELECT COUNT(1)
       INTO   v_count
       FROM   c_invoicetax t
       WHERE  t.ad_client_id           = p_client_id
       AND    t.ad_org_id              = p_org_id
       AND    t.c_invoice_id           = p_invoice_id
       AND    t.c_tax_id               = taxes.c_tax_id;
    EXCEPTION    
    WHEN TOO_MANY_ROWS THEN
        mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,2,'P','mod_billing.InvTax.Check','TOOMANY:'||p_invoice_id||':'||taxes.c_tax_id);
        RAISE g_trapped;
    WHEN NO_DATA_FOUND THEN
        v_count := 0;
    END;

    -- Update or insert lines
    IF v_count <> 0 THEN
  
       -- Update existing line  
       UPDATE c_invoicetax i
       SET    taxbaseamt     = taxes.linenetamt,
              taxamt         = taxes.taxamt
       WHERE  i.c_invoice_id = p_invoice_id
       AND    i.c_tax_id     = taxes.c_tax_id;
       mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,5,'P','mod_billing.InvLine','Updated Invoice Tax');
       
    ELSE

       -- Insert new tax line
       INSERT INTO c_invoicetax(
        c_tax_id,
        c_invoice_id,
        ad_client_id,
        ad_org_id,
        isactive,
        created,
        createdby,
        updated,
        updatedby,
        taxbaseamt,
        taxamt,
        processed,
        istaxincluded)
       VALUES(
        taxes.c_tax_id,           -- c_tax_id
        p_invoice_id,	            -- c_invoice_id
        p_client_id,              -- ad_client_id
        p_org_id,                 -- ad_org_id
        'Y',	                    -- isactive
        SYSDATE,	                -- created
        0,	                      -- createdby
        SYSDATE,	                -- updated
        0,	                      -- updatedby
        taxes.linenetamt,         -- taxbase
        taxes.taxamt,	            -- taxamt
        'N',	                    -- processed
        'N');	                  -- iistaxincluded
        mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,5,'P','mod_billing.InvHeader','Tax Line Inserted');

     END IF;

END LOOP;
         
EXCEPTION 
WHEN OTHERS THEN
     g_err := SQLCODE;
     g_errm := SQLERRM;
     mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,1,'P','mod_billing.invtax','UNKNOWN:'||g_err||':'||g_errm);
     RAISE;

END invtaxV2;

--================================================================================================
-- Create Invoice Line
--================================================================================================

PROCEDURE invline(
 p_client_id                 IN        NUMBER,
 p_org_id                    IN        NUMBER,
 p_bpartner_id               IN        NUMBER,
 p_bpartner_location_id      IN        NUMBER,
 p_description               IN        VARCHAR2,
 p_product_id                IN        NUMBER,
 p_quantity                  IN        NUMBER,
 p_pricestd                  IN        NUMBER,
 p_pricelist                 IN        NUMBER,
 p_pricelimit                IN        NUMBER,
 p_line_amt                  IN        NUMBER,
 p_num_calls                 IN        NUMBER,
 p_num_mins                  IN        NUMBER,
 p_subscription_id           IN        NUMBER,
 p_start_date                IN        DATE,
 p_end_date                  IN        DATE,
 p_cycle_date                IN        DATE,
 p_pricelist_id              IN        NUMBER,
 p_invoice_id                OUT       NUMBER,
 p_line_id                   OUT       NUMBER,
 p_run_as_date               IN        DATE,
 p_period_qty                IN        NUMBER,
 p_apply_discount            IN        VARCHAR2
)
AS

v_invoice_id                 NUMBER;
v_invoice_line_id            NUMBER;
v_line_no                    NUMBER;
v_uom_id                     NUMBER;
v_tax_id                     NUMBER;
v_tax_rate                   NUMBER;
v_tax_amt                    NUMBER;
v_attrsetinst_id             NUMBER;
v_line_total                 NUMBER;
v_istaxexempt                VARCHAR2(1);
V_TAXCATEGORY_ID             NUMBER;

BEGIN

    -- Get invoice_id
    invheader(v_invoice_id,
              p_client_id,
              p_org_id,
              p_start_date,
              p_end_date,
              p_cycle_date,
              p_bpartner_id,
              p_bpartner_location_id,
              p_run_as_date);
    
    -- Get next line id
    ad_sequence_next('C_InvoiceLine',1000000,v_invoice_line_id);

    -- Get current max line no
    SELECT NVL(max(line),0)+10
    INTO   v_line_no
    FROM   c_invoiceline l
    WHERE  l.c_invoice_id    = v_invoice_id;
    
    -- Get some data from the customer etc   
    BEGIN
       SELECT NVL(c.istaxexempt,'N')
       INTO   v_istaxexempt
       FROM   c_bpartner c
       WHERE  c.c_bpartner_id = p_bpartner_id;
    EXCEPTION 
    WHEN NO_DATA_FOUND THEN
        mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,2,'P','mod_billing.InvLine.BPartner','NOROWS:'||p_bpartner_id);
        RAISE g_trapped;
    WHEN TOO_MANY_ROWS THEN
        mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,2,'P','mod_billing.InvLine.BPartner','TOOMANY:'||p_bpartner_id);
        RAISE g_trapped;
    END;

    -- Get details from item
    BEGIN
       SELECT p.c_uom_id,
              p.c_taxcategory_id
       INTO   v_uom_id,
              v_taxcategory_id
       FROM   m_product p
       WHERE  p.m_product_id      = p_product_id
       AND    p.ad_client_id      = p_client_id;
       --AND    p.ad_org_id         IN (0, p_org_id); changes by Lavanya
    EXCEPTION 
    WHEN NO_DATA_FOUND THEN
        mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,2,'P','mod_billing.InvLine.UOM','NOROWS:'||p_product_id);
        RAISE g_trapped;
    WHEN TOO_MANY_ROWS THEN
        mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,2,'P','mod_billing.InvLine.UOM','TOOMANY:'||p_product_id);
        RAISE g_trapped;
    END;

    -- Get tax details
    BEGIN
       SELECT t.c_tax_id,
              t.rate
       INTO   v_tax_id,
              v_tax_rate
       FROM   c_tax t
       WHERE  t.ad_client_id      = p_client_id
       AND    t.ad_org_id         IN (0, p_org_id)
       AND    t.c_taxcategory_id  = v_taxcategory_id
       AND    t.istaxexempt       = v_istaxexempt
       AND    t.isactive          = 'Y';
    EXCEPTION 
    WHEN NO_DATA_FOUND THEN
        mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,2,'P','mod_billing.InvLine.TaxCat','NOROWS:'||p_product_id||'/'||v_taxcategory_id||'/'||v_istaxexempt);
        RAISE g_trapped;
    WHEN TOO_MANY_ROWS THEN
        mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,2,'P','mod_billing.InvLine.TaxCat','TOOMANY:'||p_product_id||'/'||v_taxcategory_id||'/'||v_istaxexempt);
        RAISE g_trapped;
    END;

    -- Calculate Line Amounts
    IF v_istaxexempt = 'N' THEN
      v_tax_amt      := ROUND(p_line_amt * v_tax_rate / 100,2);
    ELSE
       v_tax_amt      := 0;
    END IF;    
    v_line_total   := p_line_amt + v_tax_amt;
    mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,5,'P','mod_billing.InvLine','NumMins:'||p_num_mins);

    -- Create new attribute set instance
    mod_utils.newsetinst(g_debug,g_batch_id,v_attrsetinst_id, p_client_id, p_org_id, 'INVLINE', v_invoice_line_id, 'Subscription Occurance');
       
    -- Create new attribute instances
    mod_utils.setattr(g_debug,g_batch_id,p_client_id, p_org_id, v_attrsetinst_id, 'INVLINE', v_invoice_line_id, 'BILL_Subscription_ID', TO_CHAR(p_subscription_id));
    mod_utils.setattr(g_debug,g_batch_id,p_client_id, p_org_id, v_attrsetinst_id, 'INVLINE', v_invoice_line_id, 'BILL_Start_Date', TO_CHAR(p_start_date,'DD/MM/YY'));
    mod_utils.setattr(g_debug,g_batch_id,p_client_id, p_org_id, v_attrsetinst_id, 'INVLINE', v_invoice_line_id, 'BILL_End_Date', TO_CHAR(p_end_date,'DD/MM/YY'));
    mod_utils.setattr(g_debug,g_batch_id,p_client_id, p_org_id, v_attrsetinst_id, 'INVLINE', v_invoice_line_id, 'BILL_Pricelist_ID', p_pricelist_id);
    mod_utils.setattr(g_debug,g_batch_id,p_client_id, p_org_id, v_attrsetinst_id, 'INVLINE', v_invoice_line_id, 'BILL_Product_ID', p_product_id);
    mod_utils.setattr(g_debug,g_batch_id,p_client_id, p_org_id, v_attrsetinst_id, 'INVLINE', v_invoice_line_id, 'BILL_Num_Calls', p_num_calls);
    mod_utils.setattr(g_debug,g_batch_id,p_client_id, p_org_id, v_attrsetinst_id, 'INVLINE', v_invoice_line_id, 'BILL_Num_Mins', p_num_mins);
            
    -- Update description of attribute set instance
    MOD_UTILS.SETINSTDESCR(P_CLIENT_ID,P_ORG_ID,G_DEBUG,G_BATCH_ID,V_ATTRSETINST_ID);
    
    -- Insert Invoice Line
    INSERT INTO c_invoiceline(
     c_invoiceline_id,
     ad_client_id,
     ad_org_id,
     isactive,
     created,
     createdby,
     updated,
     updatedby,
     c_invoice_id,
     line,
     description,
     m_product_id,
     qtyinvoiced,
     pricelist,
     priceactual,
     pricelimit,
     linenetamt,
     c_uom_id,
     c_tax_id,
     taxamt,
     m_attributesetinstance_id,
     isdescription,
     isprinted,
     linetotalamt,
     processed,
     qtyentered,
     PRICEENTERED,
     RRAMT,
     periodqty)
    VALUES(
     v_invoice_line_id,                -- c_invoiceline_id
     p_client_id,	                     -- ad_client_id
     p_org_id,	                       -- ad_org_id
     'Y',	                             -- isactive
     SYSDATE,                          -- created
     0,	                               -- createdby
     SYSDATE,	                         -- updated
     0,	                               -- updatedby
     v_invoice_id,	                   -- c_invoice_id
     v_line_no,	                       -- line
     p_description,	                   -- description
     p_product_id,	                   -- m_product_id
     p_quantity,	                     -- qtyinvoiced
     p_pricelist,                      -- pricelist
     p_pricestd,	                     -- priceactual
     p_pricelimit,                     -- pricelimit
     p_line_amt,                       -- linenetamt
     v_uom_id,                         -- c_uom_id
     v_tax_id,                         -- c_tax_id
     v_tax_amt,	                       -- taxamt
     v_attrsetinst_id,                 -- m_attributesetinstance_id
     'N',	                             -- isdescription
     'Y',	                             -- isprinted
     v_line_total,                     -- linetotalamt
     'N',	                             -- processed
     p_quantity,	                     -- qtyentered
     P_PRICESTD,                       -- priceentered
     0,                                -- rramt
     p_period_qty);	                   -- periodqty
    mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,4,'P','mod_billing.InvLine','New Line Inserted');

    -- Update invoice header totals
    UPDATE c_invoice i
    SET    (chargeamt, totallines, grandtotal) =
           (SELECT SUM(l.linenetamt), SUM(l.linenetamt), round(sum(l.linenetamt+l.linenetamt*v_tax_rate/100),2) --SUM(l.linetotalamt)
            FROM   c_invoiceline l
            WHERE  l.c_invoice_id = v_invoice_id)
    WHERE  i.c_invoice_id = v_invoice_id;
    mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,5,'P','mod_billing.InvLine','Updated Invoice Header');

    -- Update invoice tax if not exempt
    IF v_istaxexempt = 'N' THEN
      -- invtax(v_invoice_id,p_client_id,p_org_id);
	 invtaxV2(v_invoice_id,p_client_id,p_org_id);
    END IF;
   
    -- Update invoice pay schedule
    UPDATE c_invoicepayschedule i
    SET    i.dueamt = 
           (SELECT SUM(l.linetotalamt)
            FROM   c_invoiceline l
            WHERE  l.c_invoice_id = v_invoice_id)
    WHERE  i.c_invoice_id = v_invoice_id;
    mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,5,'P','mod_billing.InvLine','Updated Payment Schedule');

    -- Add to counters
    g_invoice_line_count := g_invoice_line_count + 1;

    -- Return IDs
    p_invoice_id := v_invoice_id;
    p_line_id    := v_invoice_line_id;

EXCEPTION 
WHEN OTHERS THEN
     g_err := SQLCODE;
     g_errm := SQLERRM;
     mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,1,'P','mod_billing.invline','UNKNOWN:'||g_err||':'||g_errm);
     RAISE;

END invline;

--================================================================================================
-- Wrapper to call rating from Compiere 
--================================================================================================

PROCEDURE submit_rating(
 p_param_id                  NUMBER
)
IS

v_debug                      NUMBER := 3;
v_client_id                  NUMBER;
v_org_id                     NUMBER;

BEGIN

-- Set to processing
UPDATE ad_pinstance i
SET    i.isprocessing = 'Y',
       i.updated = SYSDATE
WHERE  i.ad_pinstance_id = p_param_id;
COMMIT;

-- Get Parameters
BEGIN
     SELECT i.ad_client_id,
            i.ad_org_id
     INTO   v_client_id,
            v_org_id
     FROM   ad_pinstance i
     WHERE  i.ad_pinstance_id = p_param_id;
EXCEPTION 
WHEN NO_DATA_FOUND THEN
     mod_utils.debug(v_client_id,v_org_id,g_debug,g_batch_id,2,'P','mod_billing.submit_rating.ADORGID','NOROWS');
     RAISE;
END;

-- Call Code
rating(v_debug, v_client_id, v_org_id);
COMMIT;

-- Copy log messages back to standard table
INSERT INTO ad_pinstance_log(ad_pinstance_id, log_id, p_msg)
SELECT p_param_id, ROWNUM, l.process||':'||l.mesg
FROM   mod_log l
WHERE  l.batch_id = p_param_id
ORDER BY l.log_id;

-- Set status on completion
UPDATE ad_pinstance i
SET    i.isprocessing = 'N',
       i.updated = SYSDATE,
       i.result = 1
WHERE  i.ad_pinstance_id = p_param_id;

COMMIT;

EXCEPTION 
WHEN OTHERS THEN
     g_err := SQLCODE;
     g_errm := SQLERRM;
     mod_utils.debug(v_client_id,v_org_id,g_debug,p_param_id,1,'P','mod_billing.submit_rating','UNKNOWN:'||g_err||':'||g_errm);

     -- Set status on completion
     UPDATE ad_pinstance i
     SET    i.isprocessing = 'N',
            i.updated = SYSDATE,
            i.result = 0,
            i.errormsg = g_errm
     WHERE  i.ad_pinstance_id = p_param_id;
     COMMIT;

END submit_rating;

--================================================================================================
-- Mark lines with descriptions. 
--================================================================================================

PROCEDURE rating(
 p_debug                     NUMBER,
 p_client_id                 NUMBER,
 p_org_id                    NUMBER
)
IS

----------------------
BEGIN
----------------------

-- Set debug level
g_debug  := NVL(p_debug,g_debug);

mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,0,'P','mod_billing.rating','Rating currently has no funtion. Have a nice day.');

EXCEPTION 
WHEN OTHERS THEN
     g_err := SQLCODE;
     g_errm := SQLERRM;
     ROLLBACK;
     mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,0,'P','mod_billing.rating','UNKNOWN:'||g_ref||':'||g_err||':'||g_errm);
     COMMIT;
    
END rating;

--================================================================================================
-- Select and mark calls as billed. 
--================================================================================================

PROCEDURE getcalls(
 p_client_id                 IN  NUMBER,
 p_org_id                    IN  NUMBER,
 p_attrsetinstid             IN  NUMBER,
 p_end_date                  IN  DATE,
 p_unit_pricestd             OUT NUMBER,
 p_unit_pricelist            OUT NUMBER,
 p_unit_pricelimit           OUT NUMBER,
 p_line_amt                  OUT NUMBER,
 p_temp_line_id              OUT NUMBER
)
IS

l_amount                     NUMBER;
l_username                   VARCHAR2(100);
l_sipapplicationtype         VARCHAR2(100);

----------------------
BEGIN
----------------------

-- Get unique id to tag record with
ad_sequence_next('C_InvoiceLine',1000000,p_temp_line_id);
p_temp_line_id := -p_temp_line_id;

-- Get product attributes
l_username := mod_utils.getattr(p_client_id,p_org_id,g_debug,g_batch_id,p_attrsetinstid, NULL, NULL, 'CDR_USERNAME');
l_sipapplicationtype := mod_utils.getattr(p_client_id,p_org_id,g_debug,g_batch_id,p_attrsetinstid, NULL, NULL, 'CDR_APPLICATION');

mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,5,'P','mod_billing.getcalls','AttrSetInstId:'||p_attrsetinstid);
mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,5,'P','mod_billing.getcalls','UserName:'||l_username);
mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,5,'P','mod_billing.getcalls','SipApplType:'||l_sipapplicationtype);

-- Tag calls with temp id
UPDATE mod_billing_record c
SET    c.c_invoiceline_id = p_temp_line_id
WHERE  c.ad_client_id        = p_client_id
AND    c.ad_org_id           = p_org_id
AND    c.username            = l_username
AND    UPPER(c.sipapplicationtype) = UPPER(l_sipapplicationtype)
AND    c.acctstarttime < TRUNC(p_end_date + 1)
AND    NVL(c.processed,'N')  = 'N';
        
-- Get sum of calls
SELECT SUM(NVL(c.price,0))
INTO   l_amount
FROM   mod_billing_record c
WHERE  c.c_invoiceline_id = p_temp_line_id;

-- Return results
p_unit_pricestd         := l_amount;
p_unit_pricelist        := l_amount;
p_unit_pricelimit       := l_amount;
p_line_amt              := l_amount;
p_temp_line_id          := p_temp_line_id;

EXCEPTION 
WHEN OTHERS THEN
     g_err := SQLCODE;
     g_errm := SQLERRM;
     ROLLBACK;
     mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,0,'P','mod_billing.getcalls','UNKNOWN:'||g_ref||':'||g_err||':'||g_errm);
     COMMIT;
     RAISE;
END;

--================================================================================================
-- Wrapper to call from Compiere 
--================================================================================================

PROCEDURE submit(
 p_param_id                  NUMBER
)
IS

v_debug                      NUMBER := 2;
v_client_id                  NUMBER;
v_org_id                     NUMBER;
v_run_as_date                DATE;
v_bpartner_id                NUMBER;
v_bill_calls                 VARCHAR2(1);
v_apply_discount             VARCHAR2(1);
v_status                     NUMBER;

BEGIN

-- Set Batch ID
g_batch_id := p_param_id;

-- Check Process Status
SELECT i.result
INTO   v_status
FROM   ad_pinstance i
WHERE  i.ad_pinstance_id = p_param_id;

-- Skip job is already run
IF v_status IS NOT NULL THEN
   GOTO skip_proc;
END IF;

-- Set to processing
UPDATE ad_pinstance i
SET    i.isprocessing = 'Y',
       i.updated = SYSDATE
WHERE  i.ad_pinstance_id = p_param_id;
COMMIT;

-- Get Run Date
Begin
     SELECT p.p_date,
            p.ad_client_id,
            p.ad_org_id
     INTO   v_run_as_date,
            v_client_id,
            v_org_id
     FROM   ad_pinstance_para p
     WHERE  p.ad_pinstance_id = p_param_id
     AND    UPPER(p.parametername) = 'P_RUN_DATE';
EXCEPTION 
WHEN NO_DATA_FOUND THEN
     mod_utils.debug(v_client_id,v_org_id,g_debug,g_batch_id,2,'P','mod_billing.submit.Run Date','NOROWS');
     RAISE;
END;

-- Get BPartner ID
BEGIN
     SELECT p.p_number
     INTO   v_bpartner_id
     FROM   ad_pinstance_para p
     WHERE  p.ad_pinstance_id = p_param_id
     AND    UPPER(p.parametername) = 'P_BPARTNER_ID';
EXCEPTION 
WHEN NO_DATA_FOUND THEN
     v_bpartner_id := NULL;
END;

-- Get Bill Calls Flag
BEGIN
     SELECT p.p_string
     INTO   v_bill_calls
     FROM   ad_pinstance_para p
     WHERE  p.ad_pinstance_id = p_param_id
     AND    UPPER(p.parametername) = 'P_BILL_CALLS';
EXCEPTION 
WHEN NO_DATA_FOUND THEN
     v_bill_calls := 'N';
END;

-- Get Apply Discount Flag
BEGIN
     SELECT p.p_string
     INTO   v_apply_discount
     FROM   ad_pinstance_para p
     WHERE  p.ad_pinstance_id = p_param_id
     AND    UPPER(p.parametername) = 'P_APPLY_DISCOUNT';
EXCEPTION 
WHEN NO_DATA_FOUND THEN
     v_bill_calls := 'N';
END;

-- Call Main Code
main(v_debug, v_client_id, v_org_id, v_run_as_date,v_bpartner_id, v_bill_calls, v_apply_discount);
COMMIT;

-- Copy log messages back to standard table
INSERT INTO ad_pinstance_log(ad_pinstance_id, log_id, p_msg)
SELECT z.batch_id, z.log_id, z.process||':'||z.mesg
FROM   mod_log z
WHERE  z.batch_id = p_param_id;

-- Set status on completion
UPDATE ad_pinstance i
SET    i.isprocessing = 'N',
       i.updated = SYSDATE,
       i.result = 1
WHERE  i.ad_pinstance_id = p_param_id;

-- Hump here if process instance already run
<< skip_proc >>
NULL;

COMMIT;

EXCEPTION 
WHEN OTHERS THEN
     g_err := SQLCODE;
     g_errm := SQLERRM;
     mod_utils.debug(v_client_id,v_org_id,g_debug,p_param_id,1,'P','mod_billing.submit','UNKNOWN:'||g_err||':'||g_errm);

     -- Set status on completion
     UPDATE ad_pinstance i
     SET    i.isprocessing = 'N',
            i.updated = SYSDATE,
            i.result = 0,
            i.errormsg = g_errm
     WHERE  i.ad_pinstance_id = p_param_id;
     COMMIT;

END submit;

--================================================================================================
-- Main process. 
--================================================================================================

PROCEDURE main(
 p_debug                     NUMBER,
 p_client_id                 NUMBER,
 p_org_id                    NUMBER,
 p_run_as_date               DATE, -- Billing run date. ie make bills for that biling schedule.
 p_bpartner_id               NUMBER DEFAULT NULL,
 p_bill_calls                VARCHAR2,
 p_apply_discount            VARCHAR2
)
IS

-- Select all cycles in last month
CURSOR cycle_c IS
SELECT (ADD_MONTHS(p_run_as_date,-1) + ROWNUM) run_date
FROM   c_invoiceschedule s
WHERE  ADD_MONTHS(p_run_as_date,-1) + ROWNUM < = p_run_as_date;

-- Select susbcsriptions for billing.
-- Select subs where ...
--     Customer billing schedule is day of p_run_date
--     Sub Paid until date is before p_run_date
CURSOR sub_c (i_run_date DATE) IS
SELECT s.c_bpartner_id,
       s.c_bpartner_location_id,
       s.c_subscription_id,
       s.m_product_id,
       s.bill_in_advance,
       t.frequencytype,
       t.frequency,
       s.startdate,
       s.paiduntildate,
       s.renewaldate,
       c.m_pricelist_id,
       i.invoiceday,
       i.invoicedaycutoff,
       s.name                                              sub_name,
       p.name                                              prod_name,
       p.m_attributesetinstance_id,
       NVL(s.qty,1)                                        bill_qty,
       NVL(s.ever_billed,'N')                              ever_billed
FROM   c_subscription s,
       c_subscriptiontype t,
       c_bpartner c,
       c_invoiceschedule i,
       m_product p
WHERE  s.ad_client_id                  = p_client_id
AND    s.ad_org_id                     = p_org_id
AND    s.c_subscriptiontype_id         = t.c_subscriptiontype_id
AND    s.c_bpartner_id                 = c.c_bpartner_id
AND    NVL(c.billing_start_date, TO_DATE('1-Jan-1990','dd-mon-yyyy')) <= SYSDATE()
AND    s.paiduntildate
       < DECODE(NVL(s.bill_in_advance,'N'),
                'Y',DECODE(t.frequencytype,
                           'N',ADD_MONTHS(i_run_date,t.frequency),
                           'D',(i_run_date + t.frequency),
                           'Z',s.renewaldate,
                           NULL),
                i_run_date)
AND    c.c_invoiceschedule_id          = i.c_invoiceschedule_id
AND    TO_NUMBER(DECODE(SIGN(28-i.invoiceday),
              -1,DECODE(SIGN(TO_CHAR(LAST_DAY(i_run_date),'DD')-i.invoiceday),
                        -1,TO_CHAR(LAST_DAY(i_run_date),'DD'),
                        i.invoiceday),
              i.invoiceday))
                                       = TO_NUMBER(TO_CHAR(i_run_date,'DD'))
AND    s.m_product_id                  = p.m_product_id
AND    c.c_bpartner_id                 = NVL(p_bpartner_id,c.c_bpartner_id)
AND    NVL(s.isactive,'N')             = 'Y'
AND    NVL(c.isactive,'N')             = 'Y'
AND    NVL(p.isactive,'N')             = 'Y'
ORDER BY s.c_bpartner_id,
         t.frequencytype,
         s.c_subscription_id;
       
v_qty                        NUMBER;
v_unit_pricestd              NUMBER;
v_unit_pricelist             NUMBER;
v_unit_pricelimit            NUMBER;
v_line_amt                   NUMBER;
v_line_description           VARCHAR2(100);
v_line_start_date            DATE;
v_prev_bpartner_id           NUMBER;
v_skip                       BOOLEAN;
v_offset                     NUMBER;
v_invoice_id                 NUMBER;
v_line_id                    NUMBER;
v_sql_cnt                    NUMBER;
v_pay_thru_date              DATE;
v_temp_line_id               NUMBER;
v_period_qty                 NUMBER;

----------------------
BEGIN
----------------------

-- Set debug level
g_debug  := NVL(p_debug,g_debug);

-- Print parameters
mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,0,'P','mod_billing.main','Debug Level      : '||p_debug);
mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,0,'P','mod_billing.main','Client ID        : '||p_client_id);
mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,0,'P','mod_billing.main','Org ID           : '||p_org_id);
mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,0,'P','mod_billing.main','Run As Date      : '||p_run_as_date);
mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,0,'P','mod_billing.main','Business Partner : '||p_bpartner_id);

-- Get batch id
IF g_batch_id IS NULL THEN
   SELECT mod_billing_batch_s.nextval INTO g_batch_id FROM dual;
END IF;

-- Set some variables
g_invoice_count := 0;
g_invoice_line_count := 0;

-- Create batch header and set status
INSERT INTO mod_billing_batch(ad_client_id, ad_org_id,
       isactive, created, createdby, updated, updatedby,
       batch_id, batch_date, status)
VALUES (p_client_id, p_org_id, 
        'Y', SYSDATE, 0, SYSDATE, 0,
        g_batch_id, SYSDATE, 'STARTED');
mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,0,'P','mod_billing.main','BATCH STARTED');

-- Select all cycle days in last month

FOR cycle IN cycle_c LOOP

  mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,4,'P','mod_billing.main','RunDate:'||cycle.run_date);

  -- Set the previous customer_id. Commit on each change of customer.
  -- Skip is set when a error is found for a customer line.
  -- Logic will then skip lines until new customer shows up.
  v_prev_bpartner_id := -1;
  v_skip := FALSE;

  -- Process each subscription
  FOR sub IN sub_c (cycle.run_date) LOOP

    -- Show subscription details
    mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,5,'P','mod_billing.main','Processing Subscription:'||sub.c_subscription_id);

    -- Setup Variables
    v_pay_thru_date := NULL;
    v_temp_line_id := NULL;
    
    -- Skip call billing if needed.
    IF  sub.frequencytype = 'Y' -- Monthly Calls
    AND NVL(p_bill_calls,'N') <> 'Y' THEN -- Dont bill calls
        mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,4,'P','mod_billing.main','Skipping Calls Subscription.');
        GOTO skip_line; 
    END IF;

    -- Begin Subscription block    
    BEGIN
    
    -- If bpartner has changed then commit and remember new one
    -- else if skip on then skip lines until new bpartner found.
    IF v_prev_bpartner_id <> sub.c_bpartner_id THEN
       COMMIT;
       v_prev_bpartner_id := sub.c_bpartner_id;
       v_skip := FALSE;
    ELSE
       IF v_skip THEN
          mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,4,'P','mod_billing.main','Skipping Line.');
          GOTO skip_line;
       END IF;
    END IF;
    
    -- Calculate the offset based on diff between invoicedaycutoff and invoiceday
    IF sub.invoicedaycutoff = sub.invoiceday THEN
       v_offset := 0;
    ELSIF sub.invoicedaycutoff < sub.invoiceday THEN
       v_offset := sub.invoiceday - sub.invoicedaycutoff;
    ELSE
       v_offset := sub.invoiceday + 31 - sub.invoicedaycutoff;
    END IF;

    -- Offset can only be up to 7 days. If not then looks like an error.
    IF v_offset > 7 THEN
       mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,3,'P','mod_billing.main','Offset over Limit:'||sub.invoiceday||':'||sub.invoicedaycutoff);
       RAISE g_trapped;
    END IF;

    -- Build pay_thru_date - Ignore advance flag for one offs
    IF  NVL(sub.bill_in_advance,'N') = 'Y' 
    AND sub.frequencytype <> 'Z' THEN
       IF    sub.frequencytype = 'N' THEN
             v_pay_thru_date := (ADD_MONTHS((cycle.run_date - v_offset),sub.frequency) );
       ELSIF sub.frequencytype = 'D' THEN
             v_pay_thru_date := cycle.run_date - v_offset + sub.frequency;
       ELSE
             mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,3,'P','mod_billing.main','Invalid Frequency Type:'||sub.frequencytype);
             RAISE g_trapped;
       END IF;
    ELSE
       v_pay_thru_date := cycle.run_date - v_offset;
    END IF;
       
    -- See if this subscription has been billed before
    IF  sub.ever_billed = 'N' THEN -- new subscription
       sub.paiduntildate := sub.startdate - 1; -- Set paiduntil to start date -1 as is a new subscription
    END IF;
    
    -- If the renewal date is before the paiduntildate, we have over billed so move the pay thru date back.
    -- If the renewal date is before the pay_thru_date then set pay_thru_date to stop at renewal date
    -- Negitive qtys are OK.
        -- IF sub.renewaldate < sub.paiduntildate 
        --  OR sub.renewaldate < v_pay_thru_date THEN
        --    v_pay_thru_date := sub.renewaldate;
        -- END IF;
     IF sub.renewaldate < sub.paiduntildate THEN
          v_pay_thru_date := sub.renewaldate-1;
     ELSIF sub.renewaldate < v_pay_thru_date THEN
        v_pay_thru_date := sub.renewaldate;   
     END IF;

    -- Calculate the number of units to bill based on base days of sub type and get price
    If Sub.Frequencytype = 'N' Then -- Months
       v_period_qty := Months_Between(V_Pay_Thru_Date,Sub.Paiduntildate);
       v_qty := 1;
       IF p_apply_discount = 'Y' THEN
  	     getdiscountedprice(p_client_id,p_org_id,sub.m_pricelist_id, sub.m_product_id, cycle.run_date, sub.c_bpartner_id, sub.bill_qty, v_unit_pricestd, v_unit_pricelist, v_unit_pricelimit);
       ELSE
  	     getprice(p_client_id,p_org_id,sub.m_pricelist_id, sub.m_product_id, cycle.run_date, v_unit_pricestd, v_unit_pricelist, v_unit_pricelimit);
       END IF;
    ELSIF sub.frequencytype = 'D' THEN -- Days
       v_period_qty := v_pay_thru_date - sub.paiduntildate;
        v_qty := 1;
       IF p_apply_discount = 'Y' THEN
  	     getdiscountedprice(p_client_id,p_org_id,sub.m_pricelist_id, sub.m_product_id, cycle.run_date, sub.c_bpartner_id, sub.bill_qty, v_unit_pricestd, v_unit_pricelist, v_unit_pricelimit);
       ELSE
  	     getprice(p_client_id,p_org_id,sub.m_pricelist_id, sub.m_product_id, cycle.run_date, v_unit_pricestd, v_unit_pricelist, v_unit_pricelimit);
       END IF;
    ELSIF sub.frequencytype = 'Z' THEN -- One Off Charge
       v_pay_thru_date := sub.renewaldate; -- Set pay thru to end date
       -- If weve already billed this dont charge again.
       If sub.Ever_Billed = 'Y' Then
          v_qty := 0;
          v_period_qty := 1;
       ELSE
          v_qty := 1; -- Something to bill
           v_period_qty := 1;
          IF p_apply_discount = 'Y' THEN
  	     getdiscountedprice(p_client_id,p_org_id,sub.m_pricelist_id, sub.m_product_id, cycle.run_date, sub.c_bpartner_id, sub.bill_qty, v_unit_pricestd, v_unit_pricelist, v_unit_pricelimit);
       ELSE
  	     getprice(p_client_id,p_org_id,sub.m_pricelist_id, sub.m_product_id, cycle.run_date, v_unit_pricestd, v_unit_pricelist, v_unit_pricelimit);
       END IF;
       END IF;
    ELSIF sub.frequencytype = 'Y' THEN -- Monthly Calls
       getcalls(p_client_id,p_org_id,sub.m_attributesetinstance_id,v_pay_thru_date,v_unit_pricestd,v_unit_pricelist,v_unit_pricelimit,v_line_amt,v_temp_line_id);
       IF v_unit_pricestd IS NOT NULL THEN
          v_qty := 1; -- Something to bill
          v_period_qty := 1;
       ELSE
          mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,3,'P','mod_billing.main','No Calls');
          v_qty := 0; -- Nothing to bill
          v_period_qty := 1;
       END IF;
    ELSE
       mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,3,'P','mod_billing.main','Invalid Frequency Type:'||sub.frequencytype);
       RAISE g_trapped;
    END IF;

    -- Apply multiplier to qty
    v_qty := v_qty * sub.bill_qty;
    
    -- Set the start date for line descriptions.
    v_line_start_date := sub.paiduntildate + 1;
    
    -- Build invoice line description. Set description based on frequency type
    IF sub.frequencytype = 'N' THEN -- Months 
       v_line_description := sub.sub_name||' : '||TO_CHAR(v_line_start_date,'DD/MM/YY')||' - '||TO_CHAR(v_pay_thru_date,'DD/MM/YY');
    ELSIF sub.frequencytype = 'D' THEN -- Days
       v_line_description := sub.sub_name||' : '||TO_CHAR(v_line_start_date,'DD/MM/YY')||' - '||TO_CHAR(v_pay_thru_date,'DD/MM/YY');
    ELSIF sub.frequencytype = 'Y' THEN -- Monthly Calls
       v_line_description := sub.sub_name||' : Activity to '||TO_CHAR(v_pay_thru_date,'DD/MM/YY');
    ELSIF sub.frequencytype = 'Z' THEN -- One Off
       v_line_description := sub.sub_name;
    ELSE
       mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,3,'P','mod_billing.main','Unknown Frequency Type:'||sub.frequencytype);
       RAISE g_trapped;
    END IF;

    -- Create new invoice lines for non zero qty lines
    IF v_qty <> 0 and v_period_qty <> 0 THEN

       -- Calc Line Amount
       v_line_amt := ROUND(v_qty * v_period_qty * v_unit_pricestd,2);
       
       -- Insert Line
       invline(p_client_id,
            p_org_id,
            sub.c_bpartner_id,
            sub.c_bpartner_location_id,
            v_line_description,
            sub.m_product_id,
            sub.bill_qty,
            v_unit_pricestd,
            v_unit_pricelist,
            v_unit_pricelimit,
            v_line_amt,
            NULL,
            NULL,
            sub.c_subscription_id,
            v_line_start_date,
            v_pay_thru_date,
            cycle.run_date,
            sub.m_pricelist_id,
            v_invoice_id,
            v_line_id,
            p_run_as_date,
            v_period_qty,
            p_apply_discount);

    END IF;

    -- Debug Output
    mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,5,'P','mod_billing.main','linetype          :'||sub.frequencytype);
    mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,5,'P','mod_billing.main','invoicedaycutoff  :'||sub.invoicedaycutoff);
    mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,5,'P','mod_billing.main','invoiceday        :'||sub.invoiceday);
    mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,5,'P','mod_billing.main','sub startdate     :'||sub.startdate);
    mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,5,'P','mod_billing.main','paiduntildate     :'||sub.paiduntildate);
    mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,5,'P','mod_billing.main','renewaldate       :'||sub.renewaldate);
    mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,5,'P','mod_billing.main','start_date        :'||v_line_start_date);
    mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,5,'P','mod_billing.main','pay_thru_date     :'||v_pay_thru_date);
    mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,5,'P','mod_billing.main','Qty               :'||v_qty);
    mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,5,'P','mod_billing.main','Std Price         :'||v_unit_pricestd);
    mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,5,'P','mod_billing.main','List Price        :'||v_unit_pricelist);
    mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,5,'P','mod_billing.main','Limit Price       :'||v_unit_pricelimit);
    mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,5,'P','mod_billing.main','Period Qty       :'||v_period_qty);
    
    -- Update processed call records if this was a calls subscription
    IF v_temp_line_id IS NOT NULL THEN

       UPDATE mod_billing_record c
       SET    c.processed        = 'Y',
              c.c_invoice_id     = v_invoice_id,
              c.c_invoiceline_id = v_line_id,
              c.updated          = SYSDATE
       WHERE  c.c_invoiceline_id = v_temp_line_id;
       
    END IF;

    -- Update subscription paid thru date
    UPDATE c_subscription s
    SET    s.paiduntildate        = v_pay_thru_date,
           s.ever_billed          = 'Y'
    WHERE  s.c_subscription_id    = sub.c_subscription_id;
    
    -- Jumps to here for all invoice line errors.
    -- Rollback all lines for this business partner (since last commit) and set skip flag.
    -- Logic will then skip all subsequent lines for this business partner.
    EXCEPTION 
    WHEN g_trapped THEN
        ROLLBACK;
        mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,5,'P','mod_billing.main','Trapped Exception:'||SQLCODE||'/'||SQLERRM);
        v_skip := TRUE;
    WHEN OTHERS THEN
        RAISE;
    END;

    << skip_line >>
    NULL;
          
  END LOOP;
  

  COMMIT;
  
END LOOP;

-- Update batch header status
UPDATE mod_billing_batch SET status = 'FINISHED' WHERE batch_id = g_batch_id;

-- Print results
mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,0,'P','mod_billing.main','INVOICES CREATED      : '||g_invoice_count);
mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,0,'P','mod_billing.main','INVOICE LINES CREATED : '||g_invoice_line_count);
mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,0,'P','mod_billing.main','BATCH FINISHED');

EXCEPTION 
WHEN OTHERS THEN
     g_err := SQLCODE;
     g_errm := SQLERRM;
     ROLLBACK;
     mod_utils.debug(p_client_id,p_org_id,g_debug,g_batch_id,0,'P','mod_billing.main','UNKNOWN:'||g_ref||':'||g_err||':'||g_errm);
     COMMIT;
    
END main;
 
END MOD_BILLING;