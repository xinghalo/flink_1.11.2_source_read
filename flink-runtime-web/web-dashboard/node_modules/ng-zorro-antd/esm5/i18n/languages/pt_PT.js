/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import Calendar from './calendar/pt_PT';
import DatePicker from './date-picker/pt_PT';
import Pagination from './pagination/pt_PT';
import TimePicker from './time-picker/pt_PT';
export default {
    locale: 'pt',
    Pagination: Pagination,
    DatePicker: DatePicker,
    TimePicker: TimePicker,
    Calendar: Calendar,
    Table: {
        filterTitle: 'Filtro',
        filterConfirm: 'Aplicar',
        filterReset: 'Reiniciar',
        selectAll: 'Selecionar página atual',
        selectInvert: 'Inverter seleção',
    },
    Modal: {
        okText: 'OK',
        cancelText: 'Cancelar',
        justOkText: 'OK',
    },
    Popconfirm: {
        okText: 'OK',
        cancelText: 'Cancelar',
    },
    Transfer: {
        searchPlaceholder: 'Procurar...',
        itemUnit: 'item',
        itemsUnit: 'itens',
    },
    Upload: {
        uploading: 'A carregar...',
        removeFile: 'Remover',
        uploadError: 'Erro ao carregar',
        previewFile: 'Pré-visualizar',
    },
    Empty: {
        description: 'Sem resultados',
    },
};
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicHRfUFQuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsiaTE4bi9sYW5ndWFnZXMvcHRfUFQudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7OztBQUFBLE9BQU8sUUFBUSxNQUFNLGtCQUFrQixDQUFDO0FBQ3hDLE9BQU8sVUFBVSxNQUFNLHFCQUFxQixDQUFDO0FBQzdDLE9BQU8sVUFBVSxNQUFNLG9CQUFvQixDQUFDO0FBQzVDLE9BQU8sVUFBVSxNQUFNLHFCQUFxQixDQUFDO0FBRTdDLGVBQWU7SUFDYixNQUFNLEVBQUUsSUFBSTtJQUNaLFVBQVUsWUFBQTtJQUNWLFVBQVUsWUFBQTtJQUNWLFVBQVUsWUFBQTtJQUNWLFFBQVEsVUFBQTtJQUNSLEtBQUssRUFBRTtRQUNMLFdBQVcsRUFBRSxRQUFRO1FBQ3JCLGFBQWEsRUFBRSxTQUFTO1FBQ3hCLFdBQVcsRUFBRSxXQUFXO1FBQ3hCLFNBQVMsRUFBRSx5QkFBeUI7UUFDcEMsWUFBWSxFQUFFLGtCQUFrQjtLQUNqQztJQUNELEtBQUssRUFBRTtRQUNMLE1BQU0sRUFBRSxJQUFJO1FBQ1osVUFBVSxFQUFFLFVBQVU7UUFDdEIsVUFBVSxFQUFFLElBQUk7S0FDakI7SUFDRCxVQUFVLEVBQUU7UUFDVixNQUFNLEVBQUUsSUFBSTtRQUNaLFVBQVUsRUFBRSxVQUFVO0tBQ3ZCO0lBQ0QsUUFBUSxFQUFFO1FBQ1IsaUJBQWlCLEVBQUUsYUFBYTtRQUNoQyxRQUFRLEVBQUUsTUFBTTtRQUNoQixTQUFTLEVBQUUsT0FBTztLQUNuQjtJQUNELE1BQU0sRUFBRTtRQUNOLFNBQVMsRUFBRSxlQUFlO1FBQzFCLFVBQVUsRUFBRSxTQUFTO1FBQ3JCLFdBQVcsRUFBRSxrQkFBa0I7UUFDL0IsV0FBVyxFQUFFLGdCQUFnQjtLQUM5QjtJQUNELEtBQUssRUFBRTtRQUNMLFdBQVcsRUFBRSxnQkFBZ0I7S0FDOUI7Q0FDRixDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IENhbGVuZGFyIGZyb20gJy4vY2FsZW5kYXIvcHRfUFQnO1xuaW1wb3J0IERhdGVQaWNrZXIgZnJvbSAnLi9kYXRlLXBpY2tlci9wdF9QVCc7XG5pbXBvcnQgUGFnaW5hdGlvbiBmcm9tICcuL3BhZ2luYXRpb24vcHRfUFQnO1xuaW1wb3J0IFRpbWVQaWNrZXIgZnJvbSAnLi90aW1lLXBpY2tlci9wdF9QVCc7XG5cbmV4cG9ydCBkZWZhdWx0IHtcbiAgbG9jYWxlOiAncHQnLFxuICBQYWdpbmF0aW9uLFxuICBEYXRlUGlja2VyLFxuICBUaW1lUGlja2VyLFxuICBDYWxlbmRhcixcbiAgVGFibGU6IHtcbiAgICBmaWx0ZXJUaXRsZTogJ0ZpbHRybycsXG4gICAgZmlsdGVyQ29uZmlybTogJ0FwbGljYXInLFxuICAgIGZpbHRlclJlc2V0OiAnUmVpbmljaWFyJyxcbiAgICBzZWxlY3RBbGw6ICdTZWxlY2lvbmFyIHDDoWdpbmEgYXR1YWwnLFxuICAgIHNlbGVjdEludmVydDogJ0ludmVydGVyIHNlbGXDp8OjbycsXG4gIH0sXG4gIE1vZGFsOiB7XG4gICAgb2tUZXh0OiAnT0snLFxuICAgIGNhbmNlbFRleHQ6ICdDYW5jZWxhcicsXG4gICAganVzdE9rVGV4dDogJ09LJyxcbiAgfSxcbiAgUG9wY29uZmlybToge1xuICAgIG9rVGV4dDogJ09LJyxcbiAgICBjYW5jZWxUZXh0OiAnQ2FuY2VsYXInLFxuICB9LFxuICBUcmFuc2Zlcjoge1xuICAgIHNlYXJjaFBsYWNlaG9sZGVyOiAnUHJvY3VyYXIuLi4nLFxuICAgIGl0ZW1Vbml0OiAnaXRlbScsXG4gICAgaXRlbXNVbml0OiAnaXRlbnMnLFxuICB9LFxuICBVcGxvYWQ6IHtcbiAgICB1cGxvYWRpbmc6ICdBIGNhcnJlZ2FyLi4uJyxcbiAgICByZW1vdmVGaWxlOiAnUmVtb3ZlcicsXG4gICAgdXBsb2FkRXJyb3I6ICdFcnJvIGFvIGNhcnJlZ2FyJyxcbiAgICBwcmV2aWV3RmlsZTogJ1Byw6ktdmlzdWFsaXphcicsXG4gIH0sXG4gIEVtcHR5OiB7XG4gICAgZGVzY3JpcHRpb246ICdTZW0gcmVzdWx0YWRvcycsXG4gIH0sXG59O1xuIl19