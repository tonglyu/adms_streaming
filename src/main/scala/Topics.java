
public enum Topics {
    PROD {
        public String arterialInventory() {
            return "adms.prod.arterial.inventory";
        }

        public String arterialRealtime() {
            return "adms.prod.arterial.realtime";
        }

        public String highwayInventory() {
            return "adms.prod.highway.inventory";
        }

        public String highwayRealtime() {
            return "adms.prod.highway.realtime";
        }

        public String transitBusInventory() {
            return "adms.prod.transit.bus.inventory";
        }

        public String transitBusRealtime() {
            return "adms.prod.transit.bus.realtime";
        }

        public String transitRailInventory() {
            return "adms.prod.transit.rail.inventory";
        }

        public String transitRailRealtime() {
            return "adms.prod.transit.rail.realtime";
        }

        public String travelLinksInventory() {
            return "adms.prod.transit.travelLinks.inventory";
        }

        public String travelLinksRealtime() {
            return "adms.prod.transit.travelLinks.realtime";
        }

        public String rampMeterInventory() {
            return "adms.prod.transit.rampMeter.inventory";
        }

        public String rampMeterRealtime() {
            return "adms.prod.transit.rampMeter.realtime";
        }

        public String cmsInventory() {
            return "adms.prod.cms.inventory";
        }

        public String cmsRealtime() {
            return "adms.prod.cms.realtime";
        }

        public String event() {
            return "adms.prod.event.event";
        }
    },
    DEV {
        public String arterialInventory() {
            return "adms.dev.arterial.inventory";
        }

        public String arterialRealtime() {
            return "adms.dev.arterial.realtime";
        }

        public String highwayInventory() {
            return "adms.dev.highway.inventory";
        }

        public String highwayRealtime() {
            return "adms.dev.highway.realtime";
        }

        public String transitBusInventory() {
            return "adms.dev.transit.bus.inventory";
        }

        public String transitBusRealtime() {
            return "adms.dev.transit.bus.realtime";
        }

        public String transitRailInventory() {
            return "adms.dev.transit.rail.inventory";
        }

        public String transitRailRealtime() {
            return "adms.dev.transit.rail.realtime";
        }

        public String travelLinksInventory() {
            return "adms.dev.transit.travelLinks.inventory";
        }

        public String travelLinksRealtime() {
            return "adms.dev.transit.travelLinks.realtime";
        }

        public String rampMeterInventory() {
            return "adms.dev.transit.rampMeter.inventory";
        }

        public String rampMeterRealtime() {
            return "adms.dev.transit.rampMeter.realtime";
        }

        public String cmsInventory() {
            return "adms.dev.cms.inventory";
        }

        public String cmsRealtime() {
            return "adms.dev.cms.realtime";
        }

        public String event() {
            return "adms.dev.event.event";
        }
    };

    abstract public String arterialInventory();

    abstract public String arterialRealtime();

    abstract public String highwayInventory();

    abstract public String highwayRealtime();

    abstract public String transitBusInventory();

    abstract public String transitBusRealtime();

    abstract public String transitRailInventory();

    abstract public String transitRailRealtime();

    abstract public String travelLinksInventory();

    abstract public String travelLinksRealtime();

    abstract public String rampMeterInventory();

    abstract public String rampMeterRealtime();

    abstract public String cmsInventory();

    abstract public String cmsRealtime();

    abstract public String event();
}
