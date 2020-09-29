/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import Calendar from './calendar/es_ES';
import DatePicker from './date-picker/es_ES';
import Pagination from './pagination/es_ES';
import TimePicker from './time-picker/es_ES';
export default {
    locale: 'es',
    Pagination: Pagination,
    DatePicker: DatePicker,
    TimePicker: TimePicker,
    Calendar: Calendar,
    Table: {
        filterTitle: 'Filtrar menú',
        filterConfirm: 'Aceptar',
        filterReset: 'Reiniciar',
        selectAll: 'Seleccionar todo',
        selectInvert: 'Invertir selección',
    },
    Modal: {
        okText: 'Aceptar',
        cancelText: 'Cancelar',
        justOkText: 'Aceptar',
    },
    Popconfirm: {
        okText: 'Aceptar',
        cancelText: 'Cancelar',
    },
    Transfer: {
        searchPlaceholder: 'Buscar aquí',
        itemUnit: 'elemento',
        itemsUnit: 'elementos',
    },
    Upload: {
        uploading: 'Subiendo...',
        removeFile: 'Eliminar archivo',
        uploadError: 'Error al subir el archivo',
        previewFile: 'Vista previa',
    },
    Empty: {
        description: 'No hay datos',
    },
};
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZXNfRVMuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsiaTE4bi9sYW5ndWFnZXMvZXNfRVMudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7OztBQUFBLE9BQU8sUUFBUSxNQUFNLGtCQUFrQixDQUFDO0FBQ3hDLE9BQU8sVUFBVSxNQUFNLHFCQUFxQixDQUFDO0FBQzdDLE9BQU8sVUFBVSxNQUFNLG9CQUFvQixDQUFDO0FBQzVDLE9BQU8sVUFBVSxNQUFNLHFCQUFxQixDQUFDO0FBRTdDLGVBQWU7SUFDYixNQUFNLEVBQUUsSUFBSTtJQUNaLFVBQVUsWUFBQTtJQUNWLFVBQVUsWUFBQTtJQUNWLFVBQVUsWUFBQTtJQUNWLFFBQVEsVUFBQTtJQUNSLEtBQUssRUFBRTtRQUNMLFdBQVcsRUFBRSxjQUFjO1FBQzNCLGFBQWEsRUFBRSxTQUFTO1FBQ3hCLFdBQVcsRUFBRSxXQUFXO1FBQ3hCLFNBQVMsRUFBRSxrQkFBa0I7UUFDN0IsWUFBWSxFQUFFLG9CQUFvQjtLQUNuQztJQUNELEtBQUssRUFBRTtRQUNMLE1BQU0sRUFBRSxTQUFTO1FBQ2pCLFVBQVUsRUFBRSxVQUFVO1FBQ3RCLFVBQVUsRUFBRSxTQUFTO0tBQ3RCO0lBQ0QsVUFBVSxFQUFFO1FBQ1YsTUFBTSxFQUFFLFNBQVM7UUFDakIsVUFBVSxFQUFFLFVBQVU7S0FDdkI7SUFDRCxRQUFRLEVBQUU7UUFDUixpQkFBaUIsRUFBRSxhQUFhO1FBQ2hDLFFBQVEsRUFBRSxVQUFVO1FBQ3BCLFNBQVMsRUFBRSxXQUFXO0tBQ3ZCO0lBQ0QsTUFBTSxFQUFFO1FBQ04sU0FBUyxFQUFFLGFBQWE7UUFDeEIsVUFBVSxFQUFFLGtCQUFrQjtRQUM5QixXQUFXLEVBQUUsMkJBQTJCO1FBQ3hDLFdBQVcsRUFBRSxjQUFjO0tBQzVCO0lBQ0QsS0FBSyxFQUFFO1FBQ0wsV0FBVyxFQUFFLGNBQWM7S0FDNUI7Q0FDRixDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IENhbGVuZGFyIGZyb20gJy4vY2FsZW5kYXIvZXNfRVMnO1xuaW1wb3J0IERhdGVQaWNrZXIgZnJvbSAnLi9kYXRlLXBpY2tlci9lc19FUyc7XG5pbXBvcnQgUGFnaW5hdGlvbiBmcm9tICcuL3BhZ2luYXRpb24vZXNfRVMnO1xuaW1wb3J0IFRpbWVQaWNrZXIgZnJvbSAnLi90aW1lLXBpY2tlci9lc19FUyc7XG5cbmV4cG9ydCBkZWZhdWx0IHtcbiAgbG9jYWxlOiAnZXMnLFxuICBQYWdpbmF0aW9uLFxuICBEYXRlUGlja2VyLFxuICBUaW1lUGlja2VyLFxuICBDYWxlbmRhcixcbiAgVGFibGU6IHtcbiAgICBmaWx0ZXJUaXRsZTogJ0ZpbHRyYXIgbWVuw7onLFxuICAgIGZpbHRlckNvbmZpcm06ICdBY2VwdGFyJyxcbiAgICBmaWx0ZXJSZXNldDogJ1JlaW5pY2lhcicsXG4gICAgc2VsZWN0QWxsOiAnU2VsZWNjaW9uYXIgdG9kbycsXG4gICAgc2VsZWN0SW52ZXJ0OiAnSW52ZXJ0aXIgc2VsZWNjacOzbicsXG4gIH0sXG4gIE1vZGFsOiB7XG4gICAgb2tUZXh0OiAnQWNlcHRhcicsXG4gICAgY2FuY2VsVGV4dDogJ0NhbmNlbGFyJyxcbiAgICBqdXN0T2tUZXh0OiAnQWNlcHRhcicsXG4gIH0sXG4gIFBvcGNvbmZpcm06IHtcbiAgICBva1RleHQ6ICdBY2VwdGFyJyxcbiAgICBjYW5jZWxUZXh0OiAnQ2FuY2VsYXInLFxuICB9LFxuICBUcmFuc2Zlcjoge1xuICAgIHNlYXJjaFBsYWNlaG9sZGVyOiAnQnVzY2FyIGFxdcOtJyxcbiAgICBpdGVtVW5pdDogJ2VsZW1lbnRvJyxcbiAgICBpdGVtc1VuaXQ6ICdlbGVtZW50b3MnLFxuICB9LFxuICBVcGxvYWQ6IHtcbiAgICB1cGxvYWRpbmc6ICdTdWJpZW5kby4uLicsXG4gICAgcmVtb3ZlRmlsZTogJ0VsaW1pbmFyIGFyY2hpdm8nLFxuICAgIHVwbG9hZEVycm9yOiAnRXJyb3IgYWwgc3ViaXIgZWwgYXJjaGl2bycsXG4gICAgcHJldmlld0ZpbGU6ICdWaXN0YSBwcmV2aWEnLFxuICB9LFxuICBFbXB0eToge1xuICAgIGRlc2NyaXB0aW9uOiAnTm8gaGF5IGRhdG9zJyxcbiAgfSxcbn07XG4iXX0=