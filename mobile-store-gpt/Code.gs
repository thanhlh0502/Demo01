function doGet(e) {
  var page = (e && e.parameter && e.parameter.page) ? e.parameter.page : "dashboard";
  var allowed = {
    dashboard: "dashboard",
    sales: "sales",
    inventory: "inventory",
    customer: "customer",
    employee: "employee",
    revenue: "revenue",
    warranty: "warranty",
    repair: "repair"
  };

  if (!allowed[page]) {
    page = "dashboard";
  }

  return HtmlService
    .createTemplateFromFile(allowed[page])
    .evaluate()
    .setTitle("Phone Store Pro")
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}
