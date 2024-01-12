    // inflatedMetrics

        /** 
        this.templateToTags = function(rootName, t) {
           // Validate Template
           isMetricTemplate = (x) => false;

           // 2. check if template has a parent

           // 3. Extract Metrics
           m = [];
           //
           t.value.metrics.forEach(x=> {
                n = rootName + "/" + x.name;
                if (isMetricTemplate(x)) {
                    // Add Child Tags (TODO: Recursive loops ??)
                    m.concat(templateToTags(n , x))
                }else {
                    x.name = n;
                    m.push(x);
                }
                return m;
           })
        } */