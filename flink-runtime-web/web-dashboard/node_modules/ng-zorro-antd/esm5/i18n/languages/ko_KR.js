/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import Calendar from './calendar/ko_KR';
import DatePicker from './date-picker/ko_KR';
import Pagination from './pagination/ko_KR';
import TimePicker from './time-picker/ko_KR';
export default {
    locale: 'ko',
    Pagination: Pagination,
    DatePicker: DatePicker,
    TimePicker: TimePicker,
    Calendar: Calendar,
    Table: {
        filterTitle: '필터 메뉴',
        filterConfirm: '확인',
        filterReset: '초기화',
        selectAll: '모두 선택',
        selectInvert: '선택 반전',
    },
    Modal: {
        okText: '확인',
        cancelText: '취소',
        justOkText: '확인',
    },
    Popconfirm: {
        okText: '확인',
        cancelText: '취소',
    },
    Transfer: {
        searchPlaceholder: '여기에 검색하세요',
        itemUnit: '개',
        itemsUnit: '개',
    },
    Upload: {
        uploading: '업로드 중...',
        removeFile: '파일 삭제',
        uploadError: '업로드 실패',
        previewFile: '파일 미리보기',
    },
    Empty: {
        description: '데이터 없음',
    },
};
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoia29fS1IuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsiaTE4bi9sYW5ndWFnZXMva29fS1IudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7OztBQUFBLE9BQU8sUUFBUSxNQUFNLGtCQUFrQixDQUFDO0FBQ3hDLE9BQU8sVUFBVSxNQUFNLHFCQUFxQixDQUFDO0FBQzdDLE9BQU8sVUFBVSxNQUFNLG9CQUFvQixDQUFDO0FBQzVDLE9BQU8sVUFBVSxNQUFNLHFCQUFxQixDQUFDO0FBRTdDLGVBQWU7SUFDYixNQUFNLEVBQUUsSUFBSTtJQUNaLFVBQVUsWUFBQTtJQUNWLFVBQVUsWUFBQTtJQUNWLFVBQVUsWUFBQTtJQUNWLFFBQVEsVUFBQTtJQUNSLEtBQUssRUFBRTtRQUNMLFdBQVcsRUFBRSxPQUFPO1FBQ3BCLGFBQWEsRUFBRSxJQUFJO1FBQ25CLFdBQVcsRUFBRSxLQUFLO1FBQ2xCLFNBQVMsRUFBRSxPQUFPO1FBQ2xCLFlBQVksRUFBRSxPQUFPO0tBQ3RCO0lBQ0QsS0FBSyxFQUFFO1FBQ0wsTUFBTSxFQUFFLElBQUk7UUFDWixVQUFVLEVBQUUsSUFBSTtRQUNoQixVQUFVLEVBQUUsSUFBSTtLQUNqQjtJQUNELFVBQVUsRUFBRTtRQUNWLE1BQU0sRUFBRSxJQUFJO1FBQ1osVUFBVSxFQUFFLElBQUk7S0FDakI7SUFDRCxRQUFRLEVBQUU7UUFDUixpQkFBaUIsRUFBRSxXQUFXO1FBQzlCLFFBQVEsRUFBRSxHQUFHO1FBQ2IsU0FBUyxFQUFFLEdBQUc7S0FDZjtJQUNELE1BQU0sRUFBRTtRQUNOLFNBQVMsRUFBRSxVQUFVO1FBQ3JCLFVBQVUsRUFBRSxPQUFPO1FBQ25CLFdBQVcsRUFBRSxRQUFRO1FBQ3JCLFdBQVcsRUFBRSxTQUFTO0tBQ3ZCO0lBQ0QsS0FBSyxFQUFFO1FBQ0wsV0FBVyxFQUFFLFFBQVE7S0FDdEI7Q0FDRixDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IENhbGVuZGFyIGZyb20gJy4vY2FsZW5kYXIva29fS1InO1xuaW1wb3J0IERhdGVQaWNrZXIgZnJvbSAnLi9kYXRlLXBpY2tlci9rb19LUic7XG5pbXBvcnQgUGFnaW5hdGlvbiBmcm9tICcuL3BhZ2luYXRpb24va29fS1InO1xuaW1wb3J0IFRpbWVQaWNrZXIgZnJvbSAnLi90aW1lLXBpY2tlci9rb19LUic7XG5cbmV4cG9ydCBkZWZhdWx0IHtcbiAgbG9jYWxlOiAna28nLFxuICBQYWdpbmF0aW9uLFxuICBEYXRlUGlja2VyLFxuICBUaW1lUGlja2VyLFxuICBDYWxlbmRhcixcbiAgVGFibGU6IHtcbiAgICBmaWx0ZXJUaXRsZTogJ+2VhO2EsCDrqZTribQnLFxuICAgIGZpbHRlckNvbmZpcm06ICftmZXsnbgnLFxuICAgIGZpbHRlclJlc2V0OiAn7LSI6riw7ZmUJyxcbiAgICBzZWxlY3RBbGw6ICfrqqjrkZAg7ISg7YOdJyxcbiAgICBzZWxlY3RJbnZlcnQ6ICfshKDtg50g67CY7KCEJyxcbiAgfSxcbiAgTW9kYWw6IHtcbiAgICBva1RleHQ6ICftmZXsnbgnLFxuICAgIGNhbmNlbFRleHQ6ICfst6jshownLFxuICAgIGp1c3RPa1RleHQ6ICftmZXsnbgnLFxuICB9LFxuICBQb3Bjb25maXJtOiB7XG4gICAgb2tUZXh0OiAn7ZmV7J24JyxcbiAgICBjYW5jZWxUZXh0OiAn7Leo7IaMJyxcbiAgfSxcbiAgVHJhbnNmZXI6IHtcbiAgICBzZWFyY2hQbGFjZWhvbGRlcjogJ+yXrOq4sOyXkCDqsoDsg4ntlZjshLjsmpQnLFxuICAgIGl0ZW1Vbml0OiAn6rCcJyxcbiAgICBpdGVtc1VuaXQ6ICfqsJwnLFxuICB9LFxuICBVcGxvYWQ6IHtcbiAgICB1cGxvYWRpbmc6ICfsl4XroZzrk5wg7KSRLi4uJyxcbiAgICByZW1vdmVGaWxlOiAn7YyM7J28IOyCreygnCcsXG4gICAgdXBsb2FkRXJyb3I6ICfsl4XroZzrk5wg7Iuk7YyoJyxcbiAgICBwcmV2aWV3RmlsZTogJ+2MjOydvCDrr7jrpqzrs7TquLAnLFxuICB9LFxuICBFbXB0eToge1xuICAgIGRlc2NyaXB0aW9uOiAn642w7J207YSwIOyXhuydjCcsXG4gIH0sXG59O1xuIl19