/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import Calendar from './calendar/et_EE';
import DatePicker from './date-picker/et_EE';
import Pagination from './pagination/et_EE';
import TimePicker from './time-picker/et_EE';
export default {
    locale: 'et',
    Pagination: Pagination,
    DatePicker: DatePicker,
    TimePicker: TimePicker,
    Calendar: Calendar,
    Table: {
        filterTitle: 'Filtri menüü',
        filterConfirm: 'OK',
        filterReset: 'Nulli',
        selectAll: 'Vali kõik',
        selectInvert: 'Inverteeri valik',
    },
    Modal: {
        okText: 'OK',
        cancelText: 'Tühista',
        justOkText: 'OK',
    },
    Popconfirm: {
        okText: 'OK',
        cancelText: 'Tühista',
    },
    Transfer: {
        searchPlaceholder: 'Otsi siit',
        itemUnit: 'kogus',
        itemsUnit: 'kogus',
    },
    Upload: {
        uploading: 'Üleslaadimine...',
        removeFile: 'Eemalda fail',
        uploadError: 'Üleslaadimise tõrge',
        previewFile: 'Faili eelvaade',
    },
    Empty: {
        description: 'Andmed puuduvad',
    },
};
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZXRfRUUuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsiaTE4bi9sYW5ndWFnZXMvZXRfRUUudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7OztBQUFBLE9BQU8sUUFBUSxNQUFNLGtCQUFrQixDQUFDO0FBQ3hDLE9BQU8sVUFBVSxNQUFNLHFCQUFxQixDQUFDO0FBQzdDLE9BQU8sVUFBVSxNQUFNLG9CQUFvQixDQUFDO0FBQzVDLE9BQU8sVUFBVSxNQUFNLHFCQUFxQixDQUFDO0FBRTdDLGVBQWU7SUFDYixNQUFNLEVBQUUsSUFBSTtJQUNaLFVBQVUsWUFBQTtJQUNWLFVBQVUsWUFBQTtJQUNWLFVBQVUsWUFBQTtJQUNWLFFBQVEsVUFBQTtJQUNSLEtBQUssRUFBRTtRQUNMLFdBQVcsRUFBRSxjQUFjO1FBQzNCLGFBQWEsRUFBRSxJQUFJO1FBQ25CLFdBQVcsRUFBRSxPQUFPO1FBQ3BCLFNBQVMsRUFBRSxXQUFXO1FBQ3RCLFlBQVksRUFBRSxrQkFBa0I7S0FDakM7SUFDRCxLQUFLLEVBQUU7UUFDTCxNQUFNLEVBQUUsSUFBSTtRQUNaLFVBQVUsRUFBRSxTQUFTO1FBQ3JCLFVBQVUsRUFBRSxJQUFJO0tBQ2pCO0lBQ0QsVUFBVSxFQUFFO1FBQ1YsTUFBTSxFQUFFLElBQUk7UUFDWixVQUFVLEVBQUUsU0FBUztLQUN0QjtJQUNELFFBQVEsRUFBRTtRQUNSLGlCQUFpQixFQUFFLFdBQVc7UUFDOUIsUUFBUSxFQUFFLE9BQU87UUFDakIsU0FBUyxFQUFFLE9BQU87S0FDbkI7SUFDRCxNQUFNLEVBQUU7UUFDTixTQUFTLEVBQUUsa0JBQWtCO1FBQzdCLFVBQVUsRUFBRSxjQUFjO1FBQzFCLFdBQVcsRUFBRSxxQkFBcUI7UUFDbEMsV0FBVyxFQUFFLGdCQUFnQjtLQUM5QjtJQUNELEtBQUssRUFBRTtRQUNMLFdBQVcsRUFBRSxpQkFBaUI7S0FDL0I7Q0FDRixDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IENhbGVuZGFyIGZyb20gJy4vY2FsZW5kYXIvZXRfRUUnO1xuaW1wb3J0IERhdGVQaWNrZXIgZnJvbSAnLi9kYXRlLXBpY2tlci9ldF9FRSc7XG5pbXBvcnQgUGFnaW5hdGlvbiBmcm9tICcuL3BhZ2luYXRpb24vZXRfRUUnO1xuaW1wb3J0IFRpbWVQaWNrZXIgZnJvbSAnLi90aW1lLXBpY2tlci9ldF9FRSc7XG5cbmV4cG9ydCBkZWZhdWx0IHtcbiAgbG9jYWxlOiAnZXQnLFxuICBQYWdpbmF0aW9uLFxuICBEYXRlUGlja2VyLFxuICBUaW1lUGlja2VyLFxuICBDYWxlbmRhcixcbiAgVGFibGU6IHtcbiAgICBmaWx0ZXJUaXRsZTogJ0ZpbHRyaSBtZW7DvMO8JyxcbiAgICBmaWx0ZXJDb25maXJtOiAnT0snLFxuICAgIGZpbHRlclJlc2V0OiAnTnVsbGknLFxuICAgIHNlbGVjdEFsbDogJ1ZhbGkga8O1aWsnLFxuICAgIHNlbGVjdEludmVydDogJ0ludmVydGVlcmkgdmFsaWsnLFxuICB9LFxuICBNb2RhbDoge1xuICAgIG9rVGV4dDogJ09LJyxcbiAgICBjYW5jZWxUZXh0OiAnVMO8aGlzdGEnLFxuICAgIGp1c3RPa1RleHQ6ICdPSycsXG4gIH0sXG4gIFBvcGNvbmZpcm06IHtcbiAgICBva1RleHQ6ICdPSycsXG4gICAgY2FuY2VsVGV4dDogJ1TDvGhpc3RhJyxcbiAgfSxcbiAgVHJhbnNmZXI6IHtcbiAgICBzZWFyY2hQbGFjZWhvbGRlcjogJ090c2kgc2lpdCcsXG4gICAgaXRlbVVuaXQ6ICdrb2d1cycsXG4gICAgaXRlbXNVbml0OiAna29ndXMnLFxuICB9LFxuICBVcGxvYWQ6IHtcbiAgICB1cGxvYWRpbmc6ICfDnGxlc2xhYWRpbWluZS4uLicsXG4gICAgcmVtb3ZlRmlsZTogJ0VlbWFsZGEgZmFpbCcsXG4gICAgdXBsb2FkRXJyb3I6ICfDnGxlc2xhYWRpbWlzZSB0w7VyZ2UnLFxuICAgIHByZXZpZXdGaWxlOiAnRmFpbGkgZWVsdmFhZGUnLFxuICB9LFxuICBFbXB0eToge1xuICAgIGRlc2NyaXB0aW9uOiAnQW5kbWVkIHB1dWR1dmFkJyxcbiAgfSxcbn07XG4iXX0=