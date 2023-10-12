use super::{multi::MultiDatatype, scalar::Datatype};

/// datatype representing either a Datatype or MultiDatatype
#[derive(Clone, Debug, serde::Serialize)]
pub enum MetaDatatype {
    /// Multi datatype
    Multi(MultiDatatype),
    /// Scalar datatype
    Scalar(Datatype),
}

impl MetaDatatype {
    /// get Datatype's associated with Datatype
    pub fn datatypes(&self) -> Vec<Datatype> {
        match self {
            MetaDatatype::Scalar(datatype) => vec![*datatype],
            MetaDatatype::Multi(multi_datatype) => multi_datatype.datatypes(),
        }
    }
}

/// cluster datatypes into MultiDatatype / ScalarDatatype groups
pub fn cluster_datatypes(dts: Vec<Datatype>) -> Vec<MetaDatatype> {
    // use MultiDatatypes that have at least 2 ScalarDatatypes in datatype list
    let mdts: Vec<MultiDatatype> = MultiDatatype::variants()
        .iter()
        .filter(|mdt| mdt.datatypes().iter().filter(|x| dts.contains(x)).count() >= 2)
        .cloned()
        .collect();
    let mdt_dts: Vec<Datatype> =
        mdts.iter().flat_map(|mdt| mdt.datatypes()).filter(|dt| dts.contains(dt)).collect();
    let other_dts: Vec<Datatype> = dts.iter().filter(|dt| !mdt_dts.contains(dt)).copied().collect();

    [
        mdts.iter().map(|mdt| MetaDatatype::Multi(*mdt)).collect::<Vec<MetaDatatype>>(),
        other_dts.into_iter().map(MetaDatatype::Scalar).collect(),
    ]
    .concat()
}
